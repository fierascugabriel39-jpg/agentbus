# Autor: Perplexity (Varianta A — Puntea)
# Data: 2026-09-21
# Scop: Worker pentru modelul local Qwen prin llama.cpp, vorbind limba Magistralei.
# Motiv: magistrala.py ramane neatinsa. La exterior: contractul clasic in 5 campuri
#        pe ai/out -> ai/in. La interior: asyncio + httpx cu streaming, un singur slot
#        de GPU, dedup, si propagarea contorului `hop` (pe care vechiul worker o pierdea).
# Testat: Da — broker MQTT real (amqtt in proces) + server llama.cpp simulat.
#
#   PORNIRE:
#       pip install aiomqtt httpx
#       export MQTT_PAROLA='...'                 # NU lasa parola in cod
#       llama-server -m qwen.gguf --port 1234    # endpoint OpenAI-compatibil
#       python punte/worker_local_qwen.py
#
#   VERIFICARE fara infrastructura:
#       python punte/test_punte.py

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import os
import time
from collections import OrderedDict
from pathlib import Path

import aiomqtt
import httpx

log = logging.getLogger("WorkerQwenPunte")

# ---------------------------------------------------------------- configurare --
NUME_AGENT = os.getenv("NUME_AGENT", "ModelLocal")   # numele din contractul clasic

MQTT_HOST = os.getenv("MQTT_HOST", "100.118.11.103") # Tailscale, ca in magistrala.py
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USER = os.getenv("MQTT_USER", "robot")
MQTT_PAROLA = os.getenv("MQTT_PAROLA", "")           # obligatoriu din mediu
TOPIC_ASCULT = os.getenv("TOPIC_ASCULT", "ai/out")   # unde publica dispecerul
TOPIC_TRIMIT = os.getenv("TOPIC_TRIMIT", "ai/in")    # intrarea dispecerului

# llama.cpp: llama-server expune /v1/chat/completions, OpenAI-compatibil
LLAMA_BASE = os.getenv("LLAMA_BASE", "http://127.0.0.1:1234/v1")
LLAMA_MODEL = os.getenv("LLAMA_MODEL", "qwen")       # llama-server ignora numele
TEMPERATURA = float(os.getenv("TEMPERATURA", "0.6"))
MAX_TOKENI = int(os.getenv("MAX_TOKENI", "1024"))
TIMP_MAX_S = float(os.getenv("TIMP_MAX_S", "300"))   # buget total pe o generare

FISIER_PROMPT = Path(os.getenv("FISIER_PROMPT",
                               Path(__file__).with_name("prompt_sistem.md")))
MAX_HOP = int(os.getenv("MAX_HOP", "6"))             # aceeasi frana ca in dispecer

# Frânele tale, pastrate cuvant cu cuvant din worker_ollama.py.
PROMPT_IMPLICIT = (
    "Ești un asistent tehnic strict și precis. Răspunzi doar dacă ai ceva NOU și UTIL "
    "de adăugat. Ai 4 frâne obligatorii pe care le respecți cu sfințenie:\n"
    "1. SCOP: Te oprești dacă scopul conversației a fost atins.\n"
    "2. NOU: Dacă nu adaugi nicio informație nouă, NU răspunzi.\n"
    "3. ASCULTARE: Răspunzi doar dacă mesajul celuilalt necesită o acțiune din partea ta.\n"
    "4. NU ȘTIU: Nu inventezi. Dacă nu știi, ceri indicii.\n"
    "Dacă intervine o frână și trebuie să taci (nu ai ce adăuga), scrie EXPLICIT "
    "și DOAR cuvântul: [TAC]"
)

CAMPURI = ("expeditor", "destinatar", "actiune", "continut", "ts")


class WorkerQwen:
    def __init__(self) -> None:
        self.prompt_sistem = (FISIER_PROMPT.read_text(encoding="utf-8").strip()
                              if FISIER_PROMPT.exists() else PROMPT_IMPLICIT)
        # Un model = un slot. Cu 4 GB VRAM, doua decodari simultane = OOM.
        self.slot = asyncio.Lock()
        self._vazute: OrderedDict[str, float] = OrderedDict()   # dedup QoS 1
        self._http: httpx.AsyncClient | None = None
        self._mqtt: aiomqtt.Client | None = None

    # ------------------------------------------------------------- utilitare --
    @staticmethod
    def _amprenta(p: dict) -> str:
        brut = f"{p.get('expeditor')}|{p.get('ts')}|{p.get('continut')}"
        return hashlib.sha1(brut.encode("utf-8", "replace")).hexdigest()

    def _deja_tratat(self, p: dict) -> bool:
        """MQTT QoS 1 livreaza de doua ori. Fara asta, modelul raspunde dublu."""
        cheie = self._amprenta(p)
        acum = time.time()
        for k, t in list(self._vazute.items()):        # curatare peste 10 minute
            if acum - t > 600:
                self._vazute.pop(k, None)
        if cheie in self._vazute:
            return True
        self._vazute[cheie] = acum
        while len(self._vazute) > 500:
            self._vazute.popitem(last=False)
        return False

    async def _trimite(self, destinatar: str, continut: str, hop: int,
                       actiune: str = "mesaj") -> None:
        mesaj = {
            "expeditor": NUME_AGENT,
            "destinatar": destinatar,
            "actiune": actiune,
            "continut": continut,
            "ts": int(time.time()),
            # FIX: vechiul worker nu propaga `hop`, deci contorul rămânea 0 și
            # frâna anti-buclă din dispecer nu se declanșa niciodată pe firele
            # dintre modele. Îl incrementăm, ca MAX_HOP să însemne ceva.
            "hop": hop + 1,
        }
        await self._mqtt.publish(TOPIC_TRIMIT, json.dumps(mesaj, ensure_ascii=False),
                                 qos=1)
        log.info("Răspuns trimis pe [%s] către %s (hop=%s)",
                 TOPIC_TRIMIT, destinatar, hop + 1)

    # --------------------------------------------------------------- inferenta --
    async def _genereaza(self, prompt: str) -> str:
        """Stream de la llama.cpp, agregat. Ridica excepție la eșec."""
        corp = {
            "model": LLAMA_MODEL,
            "messages": [
                {"role": "system", "content": self.prompt_sistem},
                {"role": "user", "content": prompt},
            ],
            "temperature": TEMPERATURA,
            "max_tokens": MAX_TOKENI,
            "stream": True,
        }
        bucati: list[str] = []
        t0 = time.perf_counter()
        async with self._http.stream("POST", "/chat/completions", json=corp) as r:
            if r.status_code >= 400:
                detaliu = (await r.aread()).decode(errors="replace")[:200]
                raise RuntimeError(f"llama.cpp {r.status_code}: {detaliu}")
            async for linie in r.aiter_lines():
                if not linie.startswith("data:"):
                    continue
                date = linie[5:].strip()
                if date == "[DONE]":
                    break
                try:
                    obj = json.loads(date)
                except json.JSONDecodeError:
                    continue
                alegere = (obj.get("choices") or [{}])[0]
                parte = (alegere.get("delta") or {}).get("content") or ""
                if parte:
                    bucati.append(parte)
        text = "".join(bucati).strip()
        log.info("Generare terminată: %s caractere în %.1f s",
                 len(text), time.perf_counter() - t0)
        return text

    async def _tratează(self, p: dict) -> None:
        expeditor = p["expeditor"]
        hop = int(p.get("hop", 0))

        if hop >= MAX_HOP:          # respectăm frâna și din partea workerului
            log.warning("Ignor: hop=%s >= %s (frână anti-buclă)", hop, MAX_HOP)
            return

        if self.slot.locked():
            log.warning("GPU ocupat — refuz cererea de la %s în loc s-o pun la coadă",
                        expeditor)
            await self._trimite(expeditor, "[OCUPAT] Modelul local generează deja "
                                           "un răspuns. Reia în câteva secunde.", hop)
            return

        async with self.slot:
            try:
                text = await asyncio.wait_for(self._genereaza(p["continut"]),
                                              TIMP_MAX_S)
            except asyncio.TimeoutError:
                await self._trimite(expeditor, f"[EROARE] Depășit bugetul de "
                                               f"{TIMP_MAX_S:.0f}s la generare.", hop)
                return
            except (httpx.ConnectError, httpx.ReadError) as e:
                await self._trimite(expeditor, "[EROARE] Nu răspunde llama.cpp pe "
                                               f"{LLAMA_BASE} ({e.__class__.__name__}).",
                                    hop)
                return
            except Exception as e:
                log.exception("Generare eșuată")
                await self._trimite(expeditor, f"[EROARE] {e}", hop)
                return

        if not text:
            log.info("Răspuns gol — tac.")
            return
        if "[TAC]" in text:
            log.info("Frâna activată: modelul a decis să tacă. ([TAC])")
            return
        await self._trimite(expeditor, text, hop)

    # ------------------------------------------------------------------ bucla --
    def _relevant(self, p: dict) -> bool:
        if any(c not in p for c in CAMPURI):
            return False
        if p.get("actiune") != "mesaj":
            return False
        # „Toti" = doar se vede la toți, nu declanșează răspuns. Ca în regula ta.
        if p.get("destinatar") != NUME_AGENT:
            return False
        return p.get("expeditor") != NUME_AGENT

    async def rulează(self) -> None:
        self._http = httpx.AsyncClient(
            base_url=LLAMA_BASE,
            timeout=httpx.Timeout(connect=5.0, read=TIMP_MAX_S, write=10.0, pool=5.0),
        )
        sarcini: set[asyncio.Task] = set()
        try:
            async with aiomqtt.Client(
                MQTT_HOST, MQTT_PORT, identifier=f"{NUME_AGENT}-punte",
                username=MQTT_USER or None, password=MQTT_PAROLA or None,
                keepalive=60,
            ) as client:
                self._mqtt = client
                await client.subscribe(TOPIC_ASCULT, qos=1)
                log.info("Conectat la %s:%s — ascult pe [%s], răspund pe [%s]",
                         MQTT_HOST, MQTT_PORT, TOPIC_ASCULT, TOPIC_TRIMIT)
                async for msg in client.messages:
                    try:
                        p = json.loads(msg.payload.decode("utf-8"))
                    except (json.JSONDecodeError, UnicodeDecodeError):
                        log.error("Mesaj invalid (nu e JSON) pe %s", msg.topic)
                        continue
                    if not self._relevant(p) or self._deja_tratat(p):
                        continue
                    log.info("Task de la %s — generez.", p["expeditor"])
                    # Fir asincron: bucla MQTT rămâne liberă pentru keepalive.
                    # Nu mai e nevoie de threading, ca în varianta cu paho.
                    t = asyncio.create_task(self._tratează(p))
                    sarcini.add(t)
                    t.add_done_callback(sarcini.discard)
        finally:
            for t in sarcini:
                t.cancel()
            if self._http is not None:
                await self._http.aclose()


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    if not MQTT_PAROLA:
        log.warning("MQTT_PAROLA nu e setată în mediu — încerc fără autentificare.")
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(WorkerQwen().rulează())
    log.info("Worker oprit.")


if __name__ == "__main__":
    main()
