"""Teste peste InMemoryTransport: nu necesita broker instalat."""
from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from agentbus import Agent, Envelope, InMemoryTransport, MsgType, RemoteError
from agentbus.transport import MqttTransport


class Echo(Agent):
    def __init__(self, name, transport, **kw):
        super().__init__(name, transport, **kw)
        for a in ("echo", "slow", "bad", "boom", "stream"):
            self.on(a, getattr(self, a))

    async def echo(self, env): return {"got": env.payload.get("x")}
    async def slow(self, env):
        await asyncio.sleep(5)
        return {"nu": "ajunge"}
    async def bad(self, env): raise ValueError("lipseste x")
    async def boom(self, env): raise RuntimeError("defect intern")
    async def stream(self, env):
        for i in range(3):
            await self.emit_progress(env, {"delta": f"d{i}"})
        return {"final": True}


@pytest.fixture
async def pair():
    bus = InMemoryTransport()
    srv = Echo("srv", bus, domain="t", max_concurrency=1)
    cli = Agent.__new__(Agent)          # client fara handlere
    Agent.__init__(cli, "cli", bus)
    await srv.start()
    await cli.start()
    yield cli, srv
    await cli.stop()


async def test_request_reply(pair):
    cli, _ = pair
    assert (await cli.ask("t", "echo", {"x": 42}))["got"] == 42


async def test_corelare_paralela(pair):
    """Zeci de cereri simultane nu isi amesteca raspunsurile."""
    cli, _ = pair
    res = await asyncio.gather(*[
        cli.ask_agent("srv", "echo", {"x": i}) for i in range(25)
    ])
    assert [r["got"] for r in res] == list(range(25))


async def test_bad_request_nu_se_reincearca(pair):
    cli, _ = pair
    with pytest.raises(RemoteError) as e:
        await cli.ask("t", "bad", {}, retries=3, timeout=2)
    assert e.value.code == "bad_request" and not e.value.retryable


async def test_handler_failed_este_retryabil(pair):
    cli, _ = pair
    with pytest.raises(RemoteError) as e:
        await cli.ask("t", "boom", {}, retries=0, timeout=2)
    assert e.value.code == "handler_failed" and e.value.retryable


async def test_backpressure(pair):
    """Cu max_concurrency=1, a doua cerere simultana e refuzata rapid."""
    cli, _ = pair
    slow = asyncio.create_task(cli.ask("t", "slow", {}, timeout=2, retries=0))
    await asyncio.sleep(0.1)
    with pytest.raises(RemoteError) as e:
        await cli.ask("t", "echo", {"x": 1}, timeout=1, retries=0)
    assert e.value.code == "no_capacity" and e.value.retryable
    slow.cancel()


async def test_timeout_local(pair):
    cli, _ = pair
    with pytest.raises(RemoteError) as e:
        await cli.ask("t", "slow", {}, timeout=0.3, retries=0)
    assert e.value.code == "timeout"


async def test_streaming(pair):
    cli, _ = pair
    out = [c async for c in cli.stream("t", "stream", {})]
    assert [c["delta"] for c in out[:3]] == ["d0", "d1", "d2"]
    assert out[-1] == {"final": True}


async def test_evenimente_fanout(pair):
    cli, srv = pair
    vazute = []
    await cli.subscribe_events("event.test.>", lambda e: vazute.append(e.payload))
    await srv.emit("event.test.ping", {"n": 1})
    await asyncio.sleep(0.05)
    assert vazute == [{"n": 1}]


def test_envelope_roundtrip():
    e = Envelope(type=MsgType.REQUEST, action="a.b", payload={"k": [1, 2]})
    d = Envelope.decode(e.encode())
    assert d.action == "a.b" and d.payload == {"k": [1, 2]} and d.type is MsgType.REQUEST


def test_mqtt_traducere_subiect():
    t = MqttTransport._to_topic
    assert t("svc.llm.generate") == "svc/llm/generate"
    assert t("inbox.planner.>") == "inbox/planner/#"
    assert t("event.robot.*") == "event/robot/+"
    assert MqttTransport._to_subject("inbox/planner/abc") == "inbox.planner.abc"
