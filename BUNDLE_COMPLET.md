# agentbus — sursă completă


## `README.md`

```markdown
# agentbus — comunicare asincronă între agenți (NATS / MQTT + asyncio)

Schelet de arhitectură pentru sisteme multi-agent unde **niciun agent nu apelează HTTP**:
totul trece prin broker, cu request-reply corelat, streaming de token-uri,
backpressure și dead letter queue.

## Straturi

```
┌──────────────────────────────────────────────┐
│ Planner / Vision / MotionCtl / LlmNode       │  logica de domeniu
├──────────────────────────────────────────────┤
│ Agent  (agent.py)                            │  publish/subscribe,
│  • request() / request_stream()              │  corelare, timeout,
│  • handlere per acțiune, semafor, DLQ        │  retry, erori
├──────────────────────────────────────────────┤
│ Envelope (envelope.py)                       │  corr_id, reply_to,
│                                              │  trace_id, deadline
├──────────────────────────────────────────────┤
│ Transport  NATS │ MQTT │ InMemory            │  singurul strat care
└──────────────────────────────────────────────┘  știe de broker
```

Schimbarea brokerului = o linie: `NatsTransport()` → `MqttTransport()` → `InMemoryTransport()`.

## Convenții de subiecte

| Subiect | Rol |
|---|---|
| `agent.<name>.rpc.<action>` | cerere către un agent anume |
| `svc.<capability>.<action>` | cerere către un grup de noduri echivalente (ex. `svc.llm.llm.generate`) |
| `inbox.<name>.<msg_id>` | inbox unic; aici ajung REPLY / ERROR / PROGRESS |
| `event.<domain>.<name>` | evenimente fan-out, fără răspuns |
| `dlq.<name>` | mesaje care au eșuat definitiv |

La NATS, `svc.llm.*` cu **queue group** dă load balancing real între noduri.
La MQTT folosește `$share/llm/svc/llm/#` (shared subscriptions, MQTT 5) și
înlocuiește `.` cu `/` în numele topicurilor.

## Corelarea request-reply

1. Emitentul creează `Envelope` cu `corr_id = msg_id` și `reply_to = inbox.<name>.<msg_id>`.
2. Înregistrează un `asyncio.Future` în `self._pending[corr_id]` și publică cererea.
3. `asyncio.wait_for` aplică timeout-ul; `deadline` absolut călătorește în plic, deci
   nodul remote știe cât buget mai are și abandonează cererile expirate.
4. La sosire pe inbox, `_on_inbox` rezolvă future-ul după `corr_id`. Răspunsurile
   orfane sau tardive se aruncă — nu se procesează niciodată de două ori.
5. Pentru streaming, `corr_id` mapează la o `asyncio.Queue`: mesajele `PROGRESS`
   se consumă cu `async for`, iar `REPLY` încheie iterația.

## Gestionarea erorilor

| Cod | Sursă | Retry |
|---|---|---|
| `bad_request` | handler ridică `ValueError` (validare) | nu |
| `unknown_action` | agentul nu are handler pentru acțiune | nu |
| `no_capacity` | semaforul e plin — refuz rapid, nu coadă infinită | da |
| `timeout` | fără răspuns, sau buget depășit local | da |
| `transport` | broker căzut la publish | da |
| `handler_failed` | excepție neprevăzută → și copie în `dlq.<name>` | da |

`request()` reîncearcă doar erorile marcate `retryable`, cu backoff exponențial
plafonat la 8 s și jitter aleator, ca să nu sincronizezi retry-urile între noduri.

Notă: o acțiune la care **nimeni** nu e abonat produce `timeout`, nu
`unknown_action` — abonarea se face per acțiune. Dacă vrei răspuns explicit,
abonează un agent „router" la `svc.llm.>` și lasă-l să întoarcă `unknown_action`.

## Idempotență

Cu QoS 1 / at-least-once, un mesaj poate ajunge de două ori. `msg_id` este
cheia de deduplicare: ține un `set` sau un LRU cu TTL de `msg_id`-uri tratate
în orice handler cu efecte secundare (mișcare de motor, scriere pe disc).

## Rulare

```bash
pip install nats-py aiomqtt          # opțional: doar brokerul pe care îl vrei
python examples/llm_delegation.py    # demo fără infrastructură

docker run -p 4222:4222 nats:latest              # NATS
docker run -p 1883:1883 eclipse-mosquitto        # MQTT
```

## Fluxul de delegare a inferenței

```
Planner                        broker                    LlmNode (llama.cpp)
  │ svc.llm.llm.generate         │                             │
  │  {prompt, max_tokens,        │                             │
  │   deadline, trace_id}        │                             │
  ├─────────────────────────────►├────────────────────────────►│  încarcă model
  │                              │  inbox.planner.<id>         │  decodează
  │◄─────────────────────────────┤◄────────────────────────────┤  PROGRESS {delta}
  │◄─────────────────────────────┤◄────────────────────────────┤  REPLY {text, latency}
  │ event.robot.plan_done        │                             │
  ├─────────────────────────────►│  (fan-out către audit/log)  │
```

Nodul LLM poate rula pe alt dispozitiv (Jetson, PC cu GPU, ESP32 doar ca sursă
de senzori): planner-ul nu știe adresa lui, doar capabilitatea `svc.llm`.
Pornești a doua instanță `LlmNode` cu alt nume și același grup, iar brokerul
împarte sarcinile automat.
```

## `AGENTS.md`

```markdown
# AGENTS.md — context pentru agentul din Antigravity

## Proiect
`agentbus` — schelet Python asyncio pentru comunicare asincronă între agenți
exclusiv prin broker de mesaje (NATS sau MQTT). Regulă absolută: **fără apeluri
HTTP directe între agenți**. Orice interacțiune trece prin subiecte pe broker.

## Structură
- `agentbus/transport.py` — contract `Transport` + NatsTransport / MqttTransport / InMemoryTransport
- `agentbus/envelope.py` — plicul de mesaj: msg_id, corr_id, reply_to, trace_id, deadline
- `agentbus/agent.py` — `Agent` abstract: request/reply corelat, streaming, retry, backpressure, DLQ
- `examples/llm_delegation.py` — flux: planner deleagă inferența unui nod LLM local
- `README.md` — arhitectura, convențiile de subiecte, tabelul de erori

## Reguli pentru orice modificare
1. Nu introduce HTTP, REST, gRPC sau socket direct între agenți. Doar broker.
2. Logica de domeniu nu are voie să importe `nats` sau `aiomqtt` — doar `Transport`.
3. Orice mesaj nou păstrează `corr_id`, `trace_id` și `deadline`.
4. Erorile se întorc ca `MsgType.ERROR` cu un `ErrorCode` existent, nu ca excepții netratate.
5. Handlerele cu efecte secundare (motoare, scriere pe disc) trebuie idempotente prin `msg_id`.
6. Cod și comentarii în română, ca în restul proiectului.
7. Verificare obligatorie înainte de a declara ceva terminat:
   `python examples/llm_delegation.py` trebuie să ruleze fără excepții.

## Mediu
Python 3.12+ (testat pe 3.14). Opțional: `pip install nats-py aiomqtt`.
Demo-ul rulează fără broker instalat (InMemoryTransport).

## Direcții de lucru posibile
- queue groups NATS / shared subscriptions MQTT pentru load balancing real
- deduplicare LRU cu TTL pe `msg_id`
- circuit breaker per nod remote
- teste pytest-asyncio peste InMemoryTransport
```

## `agentbus/__init__.py`

```python
from .agent import Agent, RemoteError
from .envelope import Envelope, ErrorCode, MsgType
from .transport import (
    InMemoryTransport,
    MqttTransport,
    NatsTransport,
    Transport,
    TransportError,
)

__all__ = [
    "Agent", "RemoteError", "Envelope", "ErrorCode", "MsgType",
    "Transport", "TransportError", "NatsTransport", "MqttTransport",
    "InMemoryTransport",
]
```

## `agentbus/envelope.py`

```python
"""Plicul de mesaj (envelope) — singurul format care circulă pe broker."""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, field, asdict
from enum import StrEnum
from typing import Any


class MsgType(StrEnum):
    REQUEST = "request"
    REPLY = "reply"
    ERROR = "error"
    EVENT = "event"
    PROGRESS = "progress"   # actualizări parțiale (streaming de token-uri)


class ErrorCode(StrEnum):
    TIMEOUT = "timeout"
    BAD_REQUEST = "bad_request"
    HANDLER_FAILED = "handler_failed"
    NO_CAPACITY = "no_capacity"
    UNKNOWN_ACTION = "unknown_action"
    TRANSPORT = "transport"


@dataclass(slots=True)
class Envelope:
    # --- corelare ---
    msg_id: str = field(default_factory=lambda: uuid.uuid4().hex)
    corr_id: str = ""          # identic în request și în toate răspunsurile lui
    reply_to: str = ""         # subiectul „inbox" unde se trimite răspunsul
    # --- rutare / conținut ---
    type: MsgType = MsgType.EVENT
    action: str = ""           # ex. "llm.generate"
    source: str = ""           # numele agentului emitent
    payload: dict[str, Any] = field(default_factory=dict)
    # --- observabilitate și control ---
    trace_id: str = ""         # se propagă pe tot lanțul de delegări
    ts: float = field(default_factory=time.time)
    deadline: float = 0.0      # timestamp absolut; după el mesajul e inutil
    attempt: int = 1
    error: dict[str, Any] | None = None

    def encode(self) -> bytes:
        return json.dumps(asdict(self), separators=(",", ":")).encode()

    @classmethod
    def decode(cls, raw: bytes) -> "Envelope":
        d = json.loads(raw.decode())
        d["type"] = MsgType(d.get("type", "event"))
        return cls(**d)

    @property
    def expired(self) -> bool:
        return bool(self.deadline) and time.time() > self.deadline

    def reply(self, payload: dict[str, Any], *, source: str,
              type: MsgType = MsgType.REPLY) -> "Envelope":
        return Envelope(
            corr_id=self.corr_id, type=type, action=self.action,
            source=source, payload=payload, trace_id=self.trace_id,
        )

    def fail(self, code: ErrorCode, message: str, *, source: str,
             retryable: bool = False) -> "Envelope":
        env = self.reply({}, source=source, type=MsgType.ERROR)
        env.error = {"code": str(code), "message": message, "retryable": retryable}
        return env
```

## `agentbus/transport.py`

```python
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

        await self._nc.subscribe(subject, cb=_cb)
        log.debug("abonat la %s", subject)


# --------------------------------------------------------------------------- #
# MQTT (aiomqtt)
# --------------------------------------------------------------------------- #
@dataclass
class MqttTransport(Transport):
    host: str = "127.0.0.1"
    port: int = 1883
    client_id: str = "agentbus"
    qos: int = 1                      # cel puțin o livrare — necesar pentru sarcini
    _client: object | None = None
    _stack: object | None = None
    _routes: list[tuple[str, Handler]] = field(default_factory=list)
    _reader: asyncio.Task | None = None

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
                for pattern, handler in self._routes:
                    if aiomqtt.Topic(topic).matches(pattern):
                        asyncio.create_task(handler(topic, bytes(msg.payload)))
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
            await self._client.publish(subject, payload, qos=self.qos)
        except Exception as exc:
            raise TransportError(f"publish {subject}: {exc}") from exc

    async def subscribe(self, subject: str, handler: Handler) -> None:
        if self._client is None:
            raise TransportError("transport neconectat")
        self._routes.append((subject, handler))
        await self._client.subscribe(subject, qos=self.qos)
        log.debug("abonat la %s", subject)


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
```

## `agentbus/agent.py`

```python
"""Agentul abstract: publish/subscribe + request-reply asincron peste broker."""

from __future__ import annotations

import asyncio
import logging
import random
import time
from abc import ABC
from typing import Any, AsyncIterator, Awaitable, Callable

from .envelope import Envelope, ErrorCode, MsgType
from .transport import Transport, TransportError

log = logging.getLogger("agentbus.agent")

ActionHandler = Callable[[Envelope], Awaitable[dict[str, Any]]]


class RemoteError(RuntimeError):
    """Eroare intoarsa explicit de agentul remote."""

    def __init__(self, code: str, message: str, retryable: bool = False) -> None:
        super().__init__(f"[{code}] {message}")
        self.code, self.retryable = code, retryable


class Agent(ABC):
    """Clasa de baza pentru orice nod din sistem.

    Conventii de subiecte:
        agent.<name>.rpc.<action>   - cereri directionate catre un agent
        svc.<capability>.<action>   - cereri catre un grup (load balancing)
        inbox.<name>.<msg_id>       - inbox unic pentru raspunsuri
        event.<domain>.<name>       - evenimente publicate (fan-out)
        dlq.<name>                  - dead letter queue
    """

    def __init__(self, name: str, transport: Transport, *,
                 max_concurrency: int = 4) -> None:
        self.name = name
        self.transport = transport
        self._handlers: dict[str, ActionHandler] = {}
        self._pending: dict[str, asyncio.Future] = {}
        self._streams: dict[str, asyncio.Queue] = {}
        self._sem = asyncio.Semaphore(max_concurrency)
        self._inflight = 0
        self._started = False

    # ---------------------------------------------------------------- setup --
    def on(self, action: str, handler: ActionHandler) -> None:
        self._handlers[action] = handler

    def handler(self, action: str):
        """Decorator: @agent.handler("llm.generate")"""
        def deco(fn: ActionHandler) -> ActionHandler:
            self.on(action, fn)
            return fn
        return deco

    @property
    def inbox(self) -> str:
        return f"inbox.{self.name}.>"

    async def start(self, *, groups: list[str] | None = None) -> None:
        if self._started:
            return
        await self.transport.connect()
        # inbox propriu pentru raspunsuri (corelarea request-reply)
        await self.transport.subscribe(self.inbox, self._on_inbox)
        # un subiect RPC per actiune expusa
        for action in self._handlers:
            await self.transport.subscribe(
                f"agent.{self.name}.rpc.{action}", self._on_request)
            for grp in groups or []:
                await self.transport.subscribe(
                    f"svc.{grp}.{action}", self._on_request)
        self._started = True
        log.info("agent %s pornit (actiuni: %s)", self.name,
                 ", ".join(self._handlers) or "-")
        await self.on_start()

    async def on_start(self) -> None:
        """Hook opcional pentru subclase (incarcare model, senzori etc.)."""

    async def stop(self) -> None:
        for fut in self._pending.values():
            if not fut.done():
                fut.cancel()
        await self.transport.close()
        self._started = False

    # ------------------------------------------------------------- publicare --
    async def emit(self, subject: str, payload: dict[str, Any], *,
                   trace_id: str = "") -> None:
        env = Envelope(type=MsgType.EVENT, action=subject, source=self.name,
                       payload=payload, trace_id=trace_id)
        await self.transport.publish(subject, env.encode())

    async def subscribe_events(self, subject: str,
                               handler: Callable[[Envelope], Awaitable[None]]) -> None:
        async def _cb(_subject: str, raw: bytes) -> None:
            try:
                await handler(Envelope.decode(raw))
            except Exception:
                log.exception("handler de eveniment a esuat pe %s", _subject)
        await self.transport.subscribe(subject, _cb)

    # --------------------------------------------------------- request-reply --
    async def request(self, subject: str, action: str, payload: dict[str, Any], *,
                      timeout: float = 30.0, retries: int = 2,
                      trace_id: str = "") -> dict[str, Any]:
        """RPC asincron peste broker. Corelare prin corr_id + inbox unic.

        Retry cu backoff exponential si jitter, doar pentru erori retryabile
        (timeout, transport, lipsa de capacitate) - nu pentru bad_request.
        """
        last: Exception | None = None
        for attempt in range(1, retries + 2):
            env = Envelope(
                type=MsgType.REQUEST, action=action, source=self.name,
                payload=payload, trace_id=trace_id or Envelope().msg_id,
                deadline=time.time() + timeout, attempt=attempt,
            )
            env.corr_id = env.msg_id
            env.reply_to = f"inbox.{self.name}.{env.msg_id}"

            fut: asyncio.Future = asyncio.get_running_loop().create_future()
            self._pending[env.corr_id] = fut
            try:
                await self.transport.publish(f"{subject}.{action}", env.encode())
                reply: Envelope = await asyncio.wait_for(fut, timeout)
                if reply.type is MsgType.ERROR:
                    e = reply.error or {}
                    raise RemoteError(e.get("code", "unknown"),
                                      e.get("message", ""),
                                      bool(e.get("retryable")))
                return reply.payload
            except asyncio.TimeoutError as exc:
                last = RemoteError(ErrorCode.TIMEOUT,
                                   f"{action} fara raspuns in {timeout}s", True)
                last.__cause__ = exc
            except TransportError as exc:
                last = RemoteError(ErrorCode.TRANSPORT, str(exc), True)
            except RemoteError as exc:
                last = exc
                if not exc.retryable:
                    raise
            finally:
                self._pending.pop(env.corr_id, None)

            if attempt <= retries:
                delay = min(8.0, 0.25 * 2 ** (attempt - 1)) * (1 + random.random())
                log.warning("reincercare %s (%s) in %.2fs", action, last, delay)
                await asyncio.sleep(delay)
        raise last  # type: ignore[misc]

    async def request_stream(self, subject: str, action: str,
                             payload: dict[str, Any], *,
                             timeout: float = 120.0,
                             trace_id: str = "") -> AsyncIterator[dict[str, Any]]:
        """Varianta de streaming: PROGRESS* urmat de REPLY (sau ERROR)."""
        env = Envelope(type=MsgType.REQUEST, action=action, source=self.name,
                       payload=payload, trace_id=trace_id,
                       deadline=time.time() + timeout)
        env.corr_id = env.msg_id
        env.reply_to = f"inbox.{self.name}.{env.msg_id}"
        q: asyncio.Queue = asyncio.Queue()
        self._streams[env.corr_id] = q
        try:
            await self.transport.publish(f"{subject}.{action}", env.encode())
            while True:
                msg: Envelope = await asyncio.wait_for(q.get(), timeout)
                if msg.type is MsgType.PROGRESS:
                    yield msg.payload
                    continue
                if msg.type is MsgType.ERROR:
                    e = msg.error or {}
                    raise RemoteError(e.get("code", "unknown"), e.get("message", ""))
                yield msg.payload
                return
        finally:
            self._streams.pop(env.corr_id, None)

    # -------------------------------------------------------------- receptie --
    async def _on_inbox(self, _subject: str, raw: bytes) -> None:
        try:
            env = Envelope.decode(raw)
        except Exception:
            log.exception("plic invalid in inbox")
            return
        q = self._streams.get(env.corr_id)
        if q is not None:
            q.put_nowait(env)
            return
        if env.type is MsgType.PROGRESS:
            return   # cerere non-streaming: actualizarile partiale se ignora
        fut = self._pending.get(env.corr_id)
        if fut is None or fut.done():
            log.debug("raspuns orfan/tardiv corr_id=%s - ignorat", env.corr_id)
            return   # cererea a expirat deja: raspunsul se arunca
        fut.set_result(env)

    async def _on_request(self, subject: str, raw: bytes) -> None:
        try:
            env = Envelope.decode(raw)
        except Exception:
            log.exception("cerere ilizibila pe %s", subject)
            return

        if env.expired:
            log.warning("cerere expirata %s (corr_id=%s) - abandonata",
                        env.action, env.corr_id)
            return

        handler = self._handlers.get(env.action)
        if handler is None:
            await self._send(env, env.fail(ErrorCode.UNKNOWN_ACTION,
                                           f"{self.name} nu trateaza {env.action}",
                                           source=self.name))
            return

        if self._sem.locked():
            # backpressure explicita: mai bine refuz rapid decat coada infinita
            await self._send(env, env.fail(ErrorCode.NO_CAPACITY,
                                           f"{self.name} saturat ({self._inflight})",
                                           source=self.name, retryable=True))
            return

        async with self._sem:
            self._inflight += 1
            budget = max(0.1, env.deadline - time.time()) if env.deadline else None
            try:
                result = await asyncio.wait_for(handler(env), budget)
                await self._send(env, env.reply(result, source=self.name))
            except asyncio.TimeoutError:
                await self._send(env, env.fail(ErrorCode.TIMEOUT,
                                               "buget de timp depasit local",
                                               source=self.name, retryable=True))
            except ValueError as exc:            # validare -> nu se reincearca
                await self._send(env, env.fail(ErrorCode.BAD_REQUEST, str(exc),
                                               source=self.name))
            except Exception as exc:
                log.exception("handler %s a esuat", env.action)
                await self._send(env, env.fail(ErrorCode.HANDLER_FAILED,
                                               repr(exc), source=self.name,
                                               retryable=True))
                await self._to_dlq(env, repr(exc))
            finally:
                self._inflight -= 1

    async def emit_progress(self, env: Envelope, payload: dict[str, Any]) -> None:
        await self._send(env, env.reply(payload, source=self.name,
                                        type=MsgType.PROGRESS))

    async def _send(self, req: Envelope, resp: Envelope) -> None:
        if not req.reply_to:
            return   # fire-and-forget: nimeni nu asteapta raspuns
        try:
            await self.transport.publish(req.reply_to, resp.encode())
        except TransportError:
            log.exception("nu s-a putut trimite raspunsul catre %s", req.reply_to)

    async def _to_dlq(self, env: Envelope, reason: str) -> None:
        try:
            dead = Envelope(type=MsgType.ERROR, action=env.action, source=self.name,
                            payload=env.payload, trace_id=env.trace_id,
                            error={"reason": reason, "corr_id": env.corr_id})
            await self.transport.publish(f"dlq.{self.name}", dead.encode())
        except TransportError:
            log.exception("DLQ indisponibil")
```

## `examples/llm_delegation.py`

```python
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
        self.on("llm.generate", self.generate)
        self.on("llm.health", self.health)

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

        # 1) RPC simplu, cu timeout + retry pe grupul de noduri LLM
        res = await self.request("svc.llm", "llm.generate",
                                 {"prompt": obiectiv, "max_tokens": 32},
                                 timeout=10, retries=2, trace_id=trace)
        print(f"[planner] rezultat: {res['text']}")
        print(f"[planner] {res['tokens']} tokeni in {res['latency_ms']} ms "
              f"pe {res['model']}")

        # 2) aceeasi delegare, dar consumand tokenii pe masura ce apar
        print("[planner] streaming:", end=" ", flush=True)
        async for chunk in self.request_stream("svc.llm", "llm.generate",
                                               {"prompt": obiectiv,
                                                "max_tokens": 4},
                                               trace_id=trace):
            print(chunk.get("delta") or "| FINAL", end=" ", flush=True)
        print()

        # 3) eroare de validare: nu se reincearca, se propaga imediat
        try:
            await self.request("svc.llm", "llm.generate", {}, timeout=5, retries=2)
        except RemoteError as exc:
            print(f"[planner] eroare asteptata: {exc} (retryable={exc.retryable})")

        # 4) actiune neinregistrata: nimeni nu e abonat -> timeout (nu unknown_action,
        #    fiindca abonarea se face per actiune; vezi README)
        try:
            await self.request("svc.llm", "llm.embed", {"text": "x"},
                               timeout=3, retries=0)
        except RemoteError as exc:
            print(f"[planner] eroare asteptata: {exc}")

        # 5) eveniment de telemetrie, fan-out, fara raspuns
        await self.emit("event.robot.plan_done",
                        {"obiectiv": obiectiv}, trace_id=trace)


async def main() -> None:
    logging.basicConfig(level=logging.WARNING,
                        format="%(levelname)s %(name)s: %(message)s")
    bus = InMemoryTransport()          # <- NatsTransport() / MqttTransport()

    llm = LlmNode("llm-jetson", bus, model="qwen2.5-3b-q4_k_m", max_concurrency=2)
    planner = Planner("planner", bus)

    await llm.start(groups=["llm"])    # ascult si pe svc.llm.*
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
```
