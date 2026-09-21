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
