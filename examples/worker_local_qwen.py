"""Worker de inferenta locala: agentbus pe broker + httpx catre llama.cpp / LM Studio.

Distinctia care conteaza:
  * INTRE agenti -> exclusiv broker (NATS/MQTT). Zero HTTP.
  * INAUNTRUL nodului -> HTTP catre 127.0.0.1:1234 e doar driverul modelului,
    ca un apel de biblioteca. Nu incalca regula 1 din AGENTS.md.

Rulare:
    pip install httpx nats-py
    python examples/worker_local_qwen.py            # NATS pe localhost
    AGENTBUS_TRANSPORT=memory python examples/worker_local_qwen.py --selftest
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentbus import Agent, Envelope, InMemoryTransport, NatsTransport

LLM_BASE = os.getenv("LLM_BASE_URL", "http://127.0.0.1:1234/v1")
MODEL = os.getenv("LLM_MODEL", "qwen2.5-7b-instruct")
SYSTEM_PROMPT_FILE = Path(os.getenv("SYSTEM_PROMPT_FILE", "prompts/system.md"))
FLUSH_MS = int(os.getenv("FLUSH_MS", "100"))   # agregarea token-ilor

log = logging.getLogger("worker.qwen")


class LocalQwenWorker(Agent):
    """Expune `svc.llm.generate` si `svc.llm.health` (plus agent.<name>.rpc.*)."""

    def __init__(self, name: str, transport, **kw) -> None:
        super().__init__(name, transport, **kw)
        self.on("generate", self.generate)
        self.on("health", self.health)
        self._http = None
        self._system = ""

    async def on_start(self) -> None:
        import httpx
        # un singur client, keep-alive; timeout de citire mare (decodarea e lenta)
        self._http = httpx.AsyncClient(
            base_url=LLM_BASE,
            timeout=httpx.Timeout(connect=5.0, read=300.0, write=10.0, pool=5.0),
        )
        p = Path(__file__).resolve().parents[1] / SYSTEM_PROMPT_FILE
        self._system = p.read_text(encoding="utf-8").strip() if p.exists() else ""
        log.info("worker %s pornit; model=%s; system_prompt=%s chars",
                 self.name, MODEL, len(self._system))

    async def stop(self) -> None:
        if self._http is not None:
            await self._http.aclose()
        await super().stop()

    async def health(self, env: Envelope) -> dict:
        try:
            r = await self._http.get("/models", timeout=5.0)
            ok = r.status_code == 200
        except Exception as exc:
            return {"ok": False, "error": repr(exc), "inflight": self._inflight}
        return {"ok": ok, "model": MODEL, "inflight": self._inflight}

    def _messages(self, env: Envelope) -> list[dict]:
        msgs: list[dict] = []
        sys_extra = env.payload.get("system")
        combined = "\n\n".join(x for x in (self._system, sys_extra) if x)
        if combined:
            msgs.append({"role": "system", "content": combined})
        msgs += env.payload.get("history") or []
        msgs.append({"role": "user", "content": env.payload["prompt"]})
        return msgs

    async def generate(self, env: Envelope) -> dict:
        prompt = env.payload.get("prompt")
        if not prompt or not isinstance(prompt, str):
            raise ValueError("payload.prompt lipseste sau nu e text")

        body = {
            "model": env.payload.get("model", MODEL),
            "messages": self._messages(env),
            "temperature": float(env.payload.get("temperature", 0.6)),
            "max_tokens": int(env.payload.get("max_tokens", 512)),
            "stream": True,
        }

        t0 = time.perf_counter()
        buf: list[str] = []          # tampon de agregare
        full: list[str] = []
        seq = 0
        last_flush = time.monotonic()
        finish_reason = None

        async def flush() -> None:
            nonlocal seq, last_flush
            if not buf:
                return
            chunk = "".join(buf)
            buf.clear()
            await self.emit_progress(env, {"delta": chunk, "seq": seq})
            seq += 1
            last_flush = time.monotonic()

        async with self._http.stream("POST", "/chat/completions", json=body) as resp:
            if resp.status_code >= 400:
                detail = (await resp.aread()).decode(errors="replace")[:300]
                if resp.status_code < 500:
                    raise ValueError(f"model refuza cererea: {detail}")
                raise RuntimeError(f"server LLM {resp.status_code}: {detail}")

            async for line in resp.aiter_lines():
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data == "[DONE]":
                    break
                try:
                    obj = json.loads(data)
                except json.JSONDecodeError:
                    continue
                choice = (obj.get("choices") or [{}])[0]
                piece = (choice.get("delta") or {}).get("content") or ""
                if piece:
                    buf.append(piece)
                    full.append(piece)
                finish_reason = choice.get("finish_reason") or finish_reason
                if (time.monotonic() - last_flush) * 1000 >= FLUSH_MS:
                    await flush()
                # respectam bugetul de timp primit in plic
                if env.expired:
                    await flush()
                    raise asyncio.TimeoutError("deadline depasit in timpul decodarii")

        await flush()
        text = "".join(full)
        return {
            "text": text,
            "model": body["model"],
            "chunks": seq,
            "chars": len(text),
            "finish_reason": finish_reason,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
        }


def build_transport():
    kind = os.getenv("AGENTBUS_TRANSPORT", "mqtt")   # MQTT e brokerul meu principal
    if kind == "memory":
        return InMemoryTransport()
    if kind == "mqtt":
        from agentbus import MqttTransport
        return MqttTransport(host=os.getenv("MQTT_HOST", "127.0.0.1"),
                             client_id="worker-qwen", share_group="llm")
    return NatsTransport(servers=[os.getenv("NATS_URL", "nats://127.0.0.1:4222")],
                         name="worker-qwen", queue_group="llm")


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--name", default=os.getenv("WORKER_NAME", "qwen-local"))
    ap.add_argument("--concurrency", type=int, default=1)  # 1 model = 1 slot
    ap.add_argument("--selftest", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")

    worker = LocalQwenWorker(args.name, build_transport(), domain="llm",
                             max_concurrency=args.concurrency)
    await worker.start()

    if args.selftest:
        class Probe(Agent):
            pass
        probe = Probe("probe", worker.transport)
        await probe.start()
        print("health:", await probe.ask("llm", "health", {}, timeout=10))
        print("stream:", end=" ", flush=True)
        async for c in probe.stream("llm", "generate",
                                    {"prompt": "Spune salut in 5 cuvinte."}):
            print(c.get("delta") or f"\nFINAL: {c}", end="", flush=True)
        print()
        await probe.stop()
        await worker.stop()
        return

    log.info("ascult pe svc.llm.generate — Ctrl+C pentru oprire")
    try:
        await asyncio.Event().wait()
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
