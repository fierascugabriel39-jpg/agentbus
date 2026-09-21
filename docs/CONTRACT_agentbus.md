# Contract agentbus — v1

> Fișier destinat Obsidian: sinteză de arhitectură validată, nu jurnal.
> Orice agent nou din sistem respectă acest contract fără excepție.

## 1. Adresare

| Formă | Când | Exemplu |
|---|---|---|
| `svc.<domeniu>.<actiune>` | delegare către un domeniu; mai multe instanțe echivalente împart sarcinile | `svc.llm.generate` |
| `agent.<nume>.rpc.<actiune>` | cerere către o instanță anume (debug, health țintit) | `agent.qwen-local.rpc.health` |
| `inbox.<nume>.<msg_id>` | răspuns, eroare, `PROGRESS`. Exclusiv instanței emitente | `inbox.planner.a91f…` |
| `event.<domeniu>.<nume>` | eveniment fan-out, fără răspuns | `event.robot.plan_done` |
| `dlq.<nume>` | mesaje eșuate definitiv | `dlq.qwen-local` |

În cod: `await self.ask("llm", "generate", {...})` pentru domeniu,
`await self.ask_agent("qwen-local", "health", {})` pentru instanță,
`async for c in self.stream("llm", "generate", {...})` pentru streaming.
Agentul își declară domeniul la construcție: `Agent(name, transport, domain="llm")`.

MQTT: traducerea `.`→`/`, `*`→`+`, `>`→`#` se face în `MqttTransport`.
Load balancing prin `$share/<grup>/` aplicat **numai** pe `svc.*`.
`inbox.*` nu intră niciodată în shared subscription — răspunsul trebuie să ajungă
exact la instanța care așteaptă.

## 2. Plicul de mesaj

Obligatorii în orice mesaj: `msg_id`, `corr_id`, `reply_to`, `trace_id`,
`deadline` (timestamp absolut), `type`, `action`, `source`, `payload`.
`trace_id` se propagă neschimbat pe tot lanțul de delegări.
`deadline` călătorește cu mesajul: nodul remote abandonează cererile expirate
în loc să ardă VRAM pe rezultate pe care nimeni nu le mai așteaptă.

## 3. Erori

| Cod | Cauză | Retry |
|---|---|---|
| `bad_request` | `ValueError` în handler, sau 4xx de la modelul local | nu |
| `unknown_action` | agentul nu are handlerul cerut | nu |
| `no_capacity` | semafor plin; refuz rapid, nu coadă infinită | da |
| `timeout` | fără răspuns, sau buget depășit local | da |
| `transport` | broker indisponibil la publish | da |
| `handler_failed` | excepție neprevăzută; copie în `dlq.<nume>` | da |

## 4. Reguli care nu se negociază

1. **Fără HTTP între agenți.** HTTP-ul către `127.0.0.1` (llama.cpp, Ollama) e
   driverul modelului dinăuntrul nodului, nu comunicare între agenți. Permis.
2. **Fără retry la inferență.** `retries=0` pe `generate`. O reîncercare pornește
   a doua decodare pe același GPU. Cu 4 GB VRAM asta e OOM.
3. **Un model, un slot.** `max_concurrency=1` pe nodurile de inferență.
4. **Idempotență pe `msg_id`.** QoS 1 livrează de două ori. Orice handler cu
   efecte secundare (motor ESP32, scriere pe disc, postare) deduplică pe `msg_id`.
5. **Zero-Trust pe acțiuni fizice.** Niciun handler care mișcă hardware sau
   trimite mesaje pe internet nu acceptă text liber. Doar JSON validat contra
   unei scheme, cu aprobare explicită. Textul modelului e propunere, nu comandă.
6. **Logica de domeniu nu importă `nats` sau `aiomqtt`.** Doar `Transport`.
7. **Obsidian nu e magistrală.** Agenții nu scriu loguri în vault. Doar sinteze
   intenționate, prin acțiune explicită.

## 5. Stare implementare

Verificat: request-reply corelat sub 25 de cereri paralele, streaming `PROGRESS`,
`bad_request` fără retry, `no_capacity` la saturare, timeout local, fan-out de
evenimente, traducere subiect↔topic MQTT. 10 teste, `python -m pytest -q`.

Neimplementat încă: deduplicare `msg_id` (LRU cu TTL), circuit breaker per nod,
gard de VRAM între nodurile de inferență, poartă de aprobare pentru acțiuni fizice.
