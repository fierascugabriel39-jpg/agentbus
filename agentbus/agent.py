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

    Conventii de subiecte (contract Standard_Lucru_AI):
        svc.<domeniu>.<actiune>     - delegare catre domeniu (load balancing)
        agent.<name>.rpc.<action>   - cerere catre o instanta anume
        inbox.<name>.<msg_id>       - inbox unic pentru raspunsuri
        event.<domain>.<name>       - evenimente publicate (fan-out)
        dlq.<name>                  - dead letter queue
    """

    def __init__(self, name: str, transport: Transport, *,
                 domain: str = "", max_concurrency: int = 4) -> None:
        self.name = name
        # domeniul de serviciu: determina `svc.<domeniu>.<actiune>`
        self.domain = domain
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

    async def start(self, *, domains: list[str] | None = None) -> None:
        if self._started:
            return
        await self.transport.connect()
        # inbox propriu pentru raspunsuri (corelarea request-reply)
        await self.transport.subscribe(self.inbox, self._on_inbox)
        # un subiect RPC per actiune expusa
        served = [d for d in ([self.domain] + (domains or [])) if d]
        for action in self._handlers:
            await self.transport.subscribe(
                f"agent.{self.name}.rpc.{action}", self._on_request)
            for dom in served:
                await self.transport.subscribe(
                    f"svc.{dom}.{action}", self._on_request)
        self._started = True
        log.info("agent %s pornit (domenii: %s | actiuni: %s)", self.name,
                 ", ".join(served) or "-", ", ".join(self._handlers) or "-")
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

    async def ask(self, domain: str, action: str, payload: dict[str, Any],
                  **kw) -> dict[str, Any]:
        """Delegare catre domeniu: publica pe `svc.<domain>.<action>`."""
        return await self.request(f"svc.{domain}", action, payload, **kw)

    async def ask_agent(self, name: str, action: str, payload: dict[str, Any],
                        **kw) -> dict[str, Any]:
        """Cerere catre o instanta anume: `agent.<name>.rpc.<action>`."""
        return await self.request(f"agent.{name}.rpc", action, payload, **kw)

    def stream(self, domain: str, action: str, payload: dict[str, Any], **kw):
        """Varianta de streaming catre domeniu."""
        return self.request_stream(f"svc.{domain}", action, payload, **kw)

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
