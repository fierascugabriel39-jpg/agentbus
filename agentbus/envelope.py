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
