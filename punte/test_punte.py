"""Verificare capat-la-capat a puntii, fara infrastructura instalata.

Porneste in proces: un broker MQTT real (amqtt) + un llama-server simulat,
apoi conduce workerul exact ca dispecerul tau: publica pe ai/out si asculta ai/in.

    python punte/test_punte.py
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PORT_BROKER = 18830
PORT_LLAMA = 11234

os.environ.update(
    MQTT_HOST="127.0.0.1", MQTT_PORT=str(PORT_BROKER),
    MQTT_USER="", MQTT_PAROLA="",
    LLAMA_BASE=f"http://127.0.0.1:{PORT_LLAMA}/v1",
    TIMP_MAX_S="20", MAX_HOP="6",
)
sys.path.insert(0, str(Path(__file__).resolve().parent))

import aiomqtt                                    # noqa: E402
from amqtt.broker import Broker                   # noqa: E402

import worker_local_qwen as w                     # noqa: E402

RASPUNS = ["Merge", " punctul", " A", "."]
MOD = {"tac": False, "eroare": False, "lent": 0.02}


class LlamaFals(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):  # liniste
        pass

    def do_POST(self):
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        if MOD["eroare"]:
            corp = b'{"error":"model incarcat pe jumatate"}'
            self.send_response(500)
            self.send_header("Content-Length", str(len(corp)))
            self.end_headers()
            self.wfile.write(corp)
            return
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.end_headers()
        bucati = ["[TAC]"] if MOD["tac"] else RASPUNS
        for b in bucati:
            data = b"data: " + json.dumps(
                {"choices": [{"delta": {"content": b}}]}).encode() + b"\n\n"
            self.wfile.write(hex(len(data))[2:].encode() + b"\r\n" + data + b"\r\n")
            self.wfile.flush()
            time.sleep(MOD["lent"])
        final = b'data: [DONE]\n\n'
        self.wfile.write(hex(len(final))[2:].encode() + b"\r\n" + final + b"\r\n")
        self.wfile.write(b"0\r\n\r\n")
        self.wfile.flush()


def mesaj(continut: str, *, expeditor="Gabriel", destinatar="ModelLocal",
          actiune="mesaj", hop=0, ts=None) -> str:
    return json.dumps({"expeditor": expeditor, "destinatar": destinatar,
                       "actiune": actiune, "continut": continut,
                       "ts": ts or int(time.time()), "hop": hop},
                      ensure_ascii=False)


async def main() -> None:
    srv = ThreadingHTTPServer(("127.0.0.1", PORT_LLAMA), LlamaFals)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

    broker = Broker({"listeners": {"default": {"type": "tcp",
                                              "bind": f"127.0.0.1:{PORT_BROKER}"}},
                     "sys_interval": 0, "auth": {"allow-anonymous": True},
                     "topic-check": {"enabled": False}})
    await broker.start()

    worker = asyncio.create_task(w.WorkerQwen().rulează())
    await asyncio.sleep(1.2)

    primite: list[dict] = []
    reusite, eșecuri = [], []

    def verifica(nume: str, cond: bool, detaliu: str = "") -> None:
        (reusite if cond else eșecuri).append(nume)
        print(f"  {'✓' if cond else '✗'} {nume}{'  ' + detaliu if detaliu else ''}")

    async with aiomqtt.Client("127.0.0.1", PORT_BROKER, identifier="dispecer-fals") as d:
        await d.subscribe("ai/in", qos=1)

        async def asculta():
            async for m in d.messages:
                primite.append(json.loads(m.payload.decode()))
        t = asyncio.create_task(asculta())

        async def așteaptă(n: int, s: float = 12.0) -> bool:
            gata = time.monotonic() + s
            while time.monotonic() < gata:
                if len(primite) >= n:
                    return True
                await asyncio.sleep(0.05)
            return False

        print("\n1) Răspuns normal pe ai/out -> ai/in")
        await d.publish("ai/out", mesaj("Zi-mi un punct."), qos=1)
        ok = await așteaptă(1)
        verifica("a răspuns", ok)
        if ok:
            r = primite[0]
            verifica("contract în 5 câmpuri",
                     all(c in r for c in w.CAMPURI), str(sorted(r))[:70])
            verifica("expeditor = ModelLocal", r["expeditor"] == "ModelLocal")
            verifica("destinatar = Gabriel", r["destinatar"] == "Gabriel")
            verifica("text agregat corect",
                     r["continut"] == "Merge punctul A.", repr(r["continut"]))
            verifica("hop propagat 0 -> 1", r.get("hop") == 1, f"hop={r.get('hop')}")

        print("\n2) Dedup: același mesaj livrat de două ori")
        primite.clear()
        m = mesaj("Duplicat.", ts=1700000000)
        await d.publish("ai/out", m, qos=1)
        await asyncio.sleep(0.3)
        await d.publish("ai/out", m, qos=1)
        await așteaptă(1)
        await asyncio.sleep(2.5)
        verifica("un singur răspuns", len(primite) == 1, f"primite={len(primite)}")

        print("\n3) Mesaje care NU trebuie să declanșeze nimic")
        primite.clear()
        await d.publish("ai/out", mesaj("Pentru toți.", destinatar="Toti"), qos=1)
        await d.publish("ai/out", mesaj("Alt agent.", destinatar="Claude"), qos=1)
        await d.publish("ai/out", mesaj("Stare.", actiune="schimbare_mod"), qos=1)
        await d.publish("ai/out", mesaj("Ecou propriu.", expeditor="ModelLocal"), qos=1)
        await d.publish("ai/out", mesaj("Buclă.", hop=6), qos=1)
        await d.publish("ai/out", "{nu e json", qos=1)
        await asyncio.sleep(3)
        verifica("a tăcut la toate", len(primite) == 0, f"primite={len(primite)}")

        print("\n4) Frâna [TAC]")
        MOD["tac"] = True
        primite.clear()
        await d.publish("ai/out", mesaj("Ceva redundant."), qos=1)
        await asyncio.sleep(3)
        verifica("nu publică nimic", len(primite) == 0, f"primite={len(primite)}")
        MOD["tac"] = False

        print("\n5) llama.cpp întoarce 500")
        MOD["eroare"] = True
        primite.clear()
        await d.publish("ai/out", mesaj("Cade serverul."), qos=1)
        ok = await așteaptă(1)
        verifica("raportează eroarea, nu moare",
                 ok and primite[0]["continut"].startswith("[EROARE]"),
                 primite[0]["continut"][:60] if ok else "")
        MOD["eroare"] = False

        print("\n6) Al doilea task în timpul generării (un model = un slot)")
        MOD["lent"] = 0.8          # generare lenta, ca pe GPU-ul real
        primite.clear()
        await d.publish("ai/out", mesaj("Prima cerere."), qos=1)
        await asyncio.sleep(0.15)
        await d.publish("ai/out", mesaj("A doua, imediat."), qos=1)
        await așteaptă(2)
        await asyncio.sleep(4)
        ocupat = [p for p in primite if p["continut"].startswith("[OCUPAT]")]
        verifica("a doua primește [OCUPAT]", len(ocupat) == 1,
                 f"răspunsuri={len(primite)}")
        MOD["lent"] = 0.02

        verifica("workerul e încă viu", not worker.done())
        t.cancel()

    worker.cancel()
    await broker.shutdown()
    srv.shutdown()

    print(f"\n{len(reusite)} reușite, {len(eșecuri)} eșecuri")
    if eșecuri:
        print("EȘUATE:", ", ".join(eșecuri))
    sys.exit(1 if eșecuri else 0)


if __name__ == "__main__":
    asyncio.run(main())
