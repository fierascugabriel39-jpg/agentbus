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
