"""Strat de transport abstract: același API peste NATS și MQTT.

Regula de aur a arhitecturii: nimic din nivelurile superioare nu știe ce broker
se folosește. Agentul vede doar publish/subscribe pe „subiecte" (subjects/topics).
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Awaitable, Callable

log = logging.getLogger("agentbus.transport")

# handler(subject, payload_bytes) -> None
Handler = Callable[[str, bytes], Awaitable[None]]


class TransportError(RuntimeError):
    """Eroare de nivel transport (conectare, publish, subscribe)."""


class Transport(ABC):
    """Contractul minim de care are nevoie un agent."""

    @abstractmethod
    async def connect(self) -> None: ...

    @abstractmethod
    async def close(self) -> None: ...

    @abstractmethod
    async def publish(self, subject: str, payload: bytes) -> None: ...

    @abstractmethod
    async def subscribe(self, subject: str, handler: Handler) -> None:
        """`subject` poate conține wildcard-uri (`*`, `>` la NATS; `+`, `#` la MQTT)."""


# --------------------------------------------------------------------------- #
# NATS
# --------------------------------------------------------------------------- #
@dataclass
class NatsTransport(Transport):
    servers: list[str] = field(default_factory=lambda: ["nats://127.0.0.1:4222"])
    name: str = "agentbus"
    queue_group: str = ""   # load balancing real pe svc.* intre noduri echivalente
    _nc: object | None = None

    async def connect(self) -> None:
        try:
            import nats  # nats-py
        except ImportError as exc:  # pragma: no cover
            raise TransportError("instalează `nats-py`") from exc
        try:
            self._nc = await nats.connect(
                servers=self.servers,
                name=self.name,
                max_reconnect_attempts=-1,      # reconectare la infinit
                reconnect_time_wait=2,
                ping_interval=10,
            )
        except Exception as exc:
            raise TransportError(f"conectare NATS eșuată: {exc}") from exc
        log.info("NATS conectat: %s", self.servers)

    async def close(self) -> None:
        if self._nc is not None:
            await self._nc.drain()  # golește subscripțiile înainte de închidere
            self._nc = None

    async def publish(self, subject: str, payload: bytes) -> None:
        if self._nc is None:
            raise TransportError("transport neconectat")
        try:
            await self._nc.publish(subject, payload)
            await self._nc.flush(timeout=2)
        except Exception as exc:
            raise TransportError(f"publish {subject}: {exc}") from exc

    async def subscribe(self, subject: str, handler: Handler) -> None:
        if self._nc is None:
            raise TransportError("transport neconectat")

        async def _cb(msg) -> None:
            await handler(msg.subject, msg.data)

        # queue group doar pe cozile de sarcini; inbox-ul ramane exclusiv
        queue = self.queue_group if (self.queue_group and
                                     subject.startswith("svc.")) else ""
        await self._nc.subscribe(subject, queue=queue, cb=_cb)
        log.debug("abonat la %s (queue=%r)", subject, queue)


# --------------------------------------------------------------------------- #
# MQTT (aiomqtt)
# --------------------------------------------------------------------------- #
@dataclass
class MqttTransport(Transport):
    host: str = "127.0.0.1"
    port: int = 1883
    client_id: str = "agentbus"
    qos: int = 1                      # cel puțin o livrare — necesar pentru sarcini
    share_group: str = ""             # MQTT 5: load balancing prin $share/<grup>/
    _client: object | None = None
    _stack: object | None = None
    _routes: list[tuple[str, Handler]] = field(default_factory=list)
    _reader: asyncio.Task | None = None

    # -- traducere subiect NATS  <->  topic MQTT ---------------------------- #
    # Agentul vorbește un singur dialect ('a.b.c', '*', '>'); aici îl convertim.
    @staticmethod
    def _to_topic(subject: str) -> str:
        table = {">": "#", "*": "+"}
        return "/".join(table.get(t, t) for t in subject.split("."))

    @staticmethod
    def _to_subject(topic: str) -> str:
        return ".".join(topic.split("/"))

    async def connect(self) -> None:
        try:
            import aiomqtt
        except ImportError as exc:  # pragma: no cover
            raise TransportError("instalează `aiomqtt`") from exc
        from contextlib import AsyncExitStack

        self._stack = AsyncExitStack()
        try:
            self._client = await self._stack.enter_async_context(
                aiomqtt.Client(self.host, self.port, identifier=self.client_id)
            )
        except Exception as exc:
            raise TransportError(f"conectare MQTT eșuată: {exc}") from exc
        self._reader = asyncio.create_task(self._dispatch_loop(), name="mqtt-reader")
        log.info("MQTT conectat: %s:%s", self.host, self.port)

    async def _dispatch_loop(self) -> None:
        import aiomqtt

        try:
            async for msg in self._client.messages:
                topic = str(msg.topic)
                subject = self._to_subject(topic)
                for pattern, handler in self._routes:
                    if aiomqtt.Topic(topic).matches(pattern):
                        # handlerul primește subiectul în dialect NATS
                        asyncio.create_task(handler(subject, bytes(msg.payload)))
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("bucla MQTT s-a oprit")

    async def close(self) -> None:
        if self._reader:
            self._reader.cancel()
        if self._stack is not None:
            await self._stack.aclose()
            self._stack = None

    async def publish(self, subject: str, payload: bytes) -> None:
        if self._client is None:
            raise TransportError("transport neconectat")
        try:
            await self._client.publish(self._to_topic(subject), payload, qos=self.qos)
        except Exception as exc:
            raise TransportError(f"publish {subject}: {exc}") from exc

    async def subscribe(self, subject: str, handler: Handler) -> None:
        if self._client is None:
            raise TransportError("transport neconectat")
        topic = self._to_topic(subject)
        self._routes.append((topic, handler))   # potrivirea se face pe topic
        # $share doar pentru cozile de sarcini (svc.*), niciodată pentru inbox:
        # inbox-ul trebuie livrat exact instanței care așteaptă răspunsul.
        if self.share_group and subject.startswith("svc."):
            topic = f"$share/{self.share_group}/{topic}"
        await self._client.subscribe(topic, qos=self.qos)
        log.debug("abonat la %s", topic)


class InMemoryTransport(Transport):
    """Broker fals, pentru teste și pentru demo fără infrastructură."""

    def __init__(self) -> None:
        self._routes: list[tuple[str, Handler]] = []

    async def connect(self) -> None: ...
    async def close(self) -> None: ...

    @staticmethod
    def _match(pattern: str, subject: str) -> bool:
        p, s = pattern.split("."), subject.split(".")
        for i, tok in enumerate(p):
            if tok in (">", "#"):
                return True
            if i >= len(s):
                return False
            if tok not in ("*", "+") and tok != s[i]:
                return False
        return len(p) == len(s)

    async def publish(self, subject: str, payload: bytes) -> None:
        for pattern, handler in list(self._routes):
            if self._match(pattern, subject):
                asyncio.create_task(handler(subject, payload))

    async def subscribe(self, subject: str, handler: Handler) -> None:
        self._routes.append((subject, handler))
