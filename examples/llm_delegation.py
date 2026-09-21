"""Flux complet: un agent de planificare deleaga inferenta unui nod LLM local.

Zero HTTP: totul trece prin broker (aici InMemoryTransport pentru demo; in
productie inlocuiesti o singura linie cu NatsTransport sau MqttTransport).

    Planner                     broker                    LlmNode (llama.cpp)
       |  svc.llm.llm.generate    |                            |
       |------------------------->|--------------------------->|
       |                          |   inbox.planner.<id>       | (progress)
       |<-------------------------|<---------------------------|
       |                          |   reply / error            |

Rulare:  python examples/llm_delegation.py
"""

from __future__ import annotations

import asyncio
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentbus import Agent, Envelope, InMemoryTransport, RemoteError


# --------------------------------------------------------------------------- #
# Nodul de inferenta locala
# --------------------------------------------------------------------------- #
class LlmNode(Agent):
    """Wrapper peste un model local (llama.cpp / Ollama / vLLM in proces)."""

    def __init__(self, name: str, transport, model: str, **kw) -> None:
        super().__init__(name, transport, **kw)
        self.model = model
        self.on("generate", self.generate)
        self.on("health", self.health)

    async def on_start(self) -> None:
        # aici ai incarca efectiv greutatile: Llama(model_path=...)
        await asyncio.sleep(0.05)
        print(f"[{self.name}] model {self.model} incarcat")

    async def health(self, env: Envelope) -> dict:
        return {"ok": True, "model": self.model, "inflight": self._inflight}

    async def generate(self, env: Envelope) -> dict:
        prompt = env.payload.get("prompt")
        if not prompt:                      # ValueError -> bad_request, fara retry
            raise ValueError("payload.prompt lipseste")

        max_tokens = int(env.payload.get("max_tokens", 64))
        t0 = time.perf_counter()
        tokens: list[str] = []
        # bucla de decodare simulata; tokenii pleaca incremental ca PROGRESS
        for i in range(min(max_tokens, 5)):
            await asyncio.sleep(0.08)
            tokens.append(f"token{i}")
            await self.emit_progress(env, {"delta": tokens[-1], "index": i})

        text = f"raspuns pentru '{prompt[:40]}' -> " + " ".join(tokens)
        return {
            "text": text,
            "model": self.model,
            "tokens": len(tokens),
            "latency_ms": round((time.perf_counter() - t0) * 1000, 1),
        }


# --------------------------------------------------------------------------- #
# Agentul care delega
# --------------------------------------------------------------------------- #
class Planner(Agent):
    async def plan(self, obiectiv: str) -> None:
        trace = Envelope().msg_id
        print(f"\n[planner] obiectiv: {obiectiv} (trace={trace[:8]})")

        # 1) RPC simplu. ATENTIE: retries=0 la generare — o reincercare ar porni
        #    o a doua decodare pe acelasi GPU (4GB VRAM nu iarta).
        res = await self.ask("llm", "generate",
                                 {"prompt": obiectiv, "max_tokens": 32},
                                 timeout=10, retries=0, trace_id=trace)
        print(f"[planner] rezultat: {res['text']}")
        print(f"[planner] {res['tokens']} tokeni in {res['latency_ms']} ms "
              f"pe {res['model']}")

        # 2) aceeasi delegare, dar consumand tokenii pe masura ce apar
        print("[planner] streaming:", end=" ", flush=True)
        async for chunk in self.stream("llm", "generate",
                                               {"prompt": obiectiv,
                                                "max_tokens": 4},
                                               trace_id=trace):
            print(chunk.get("delta") or "| FINAL", end=" ", flush=True)
        print()

        # 3) eroare de validare: nu se reincearca, se propaga imediat
        try:
            await self.ask("llm", "generate", {}, timeout=5, retries=2)
        except RemoteError as exc:
            print(f"[planner] eroare asteptata: {exc} (retryable={exc.retryable})")

        # 4) actiune neinregistrata: nimeni nu e abonat -> timeout (nu unknown_action,
        #    fiindca abonarea se face per actiune; vezi README)
        try:
            await self.ask("llm", "embed", {"text": "x"}, timeout=3, retries=0)
        except RemoteError as exc:
            print(f"[planner] eroare asteptata: {exc}")

        # 5) eveniment de telemetrie, fan-out, fara raspuns
        await self.emit("event.robot.plan_done",
                        {"obiectiv": obiectiv}, trace_id=trace)


async def main() -> None:
    logging.basicConfig(level=logging.WARNING,
                        format="%(levelname)s %(name)s: %(message)s")
    bus = InMemoryTransport()          # <- NatsTransport() / MqttTransport()

    llm = LlmNode("llm-jetson", bus, model="qwen2.5-3b-q4_k_m",
                  domain="llm", max_concurrency=2)
    planner = Planner("planner", bus)

    await llm.start()                  # ascult pe svc.llm.generate / .health
    await planner.start()

    await planner.subscribe_events(
        "event.robot.>",
        lambda e: asyncio.sleep(0, print(f"[audit] {e.action} <- {e.source}")),
    )

    await planner.plan("planifica traiectoria brațului catre piesa roșie")
    await asyncio.sleep(0.2)
    await planner.stop()


if __name__ == "__main__":
    asyncio.run(main())
