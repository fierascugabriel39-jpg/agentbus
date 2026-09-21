# Puntea — worker Qwen local pe Magistrala existentă (Varianta A)

`magistrala.py` rămâne neatins. Workerul vorbește contractul clasic în 5 câmpuri
pe `ai/out` → `ai/in`, dar pe dinăuntru e asyncio + httpx cu streaming.

## Pornire

```bash
pip install aiomqtt httpx
export MQTT_PAROLA='parola_nouă'        # NU o lăsa în cod
llama-server -m qwen-abliterat.gguf --port 1234 -ngl 99
python punte/worker_local_qwen.py
```

Dacă modelul rulează pe altă mașină sau alt port: `export LLAMA_BASE=http://IP:PORT/v1`.
Opriți workerul vechi (`worker_ollama.py`) înainte — altfel răspund amândoi.

## Verificare fără infrastructură

```bash
pip install amqtt
python punte/test_punte.py
```

Pornește în proces un broker MQTT real și un llama-server simulat, apoi conduce
workerul exact ca dispecerul. 12 verificări: răspuns normal, contractul în 5 câmpuri,
`hop` propagat, dedup la livrare dublă, tăcere la `Toti`/alt destinatar/`schimbare_mod`/
ecou propriu/`hop` la limită/JSON invalid, frâna `[TAC]`, eroare 500 de la model,
refuz `[OCUPAT]` la a doua cerere simultană.

## Ce e diferit de `worker_ollama.py`

| | vechi | punte |
|---|---|---|
| I/O | `paho` sincron + `threading.Thread` per cerere | asyncio, un singur fir |
| Model | Ollama `/api/generate`, `stream=False` | llama.cpp `/v1/chat/completions`, stream |
| `hop` | nu era propagat — frâna anti-buclă nu se declanșa niciodată | `hop + 1` la fiecare răspuns |
| Cereri simultane | porneau două generări pe același GPU | a doua primește `[OCUPAT]` |
| Livrare dublă (QoS 1) | răspundea de două ori | dedup pe amprentă, 10 minute |
| Parolă | în clar în cod | din `MQTT_PAROLA` |
| Timeout | 60 s fix | buget configurabil, raportat ca `[EROARE]` |

Frânele și `[TAC]` sunt păstrate cuvânt cu cuvânt, în `punte/prompt_sistem.md`.

## Ce NU face

Nu trimite `PROGRESS` / răspuns pe bucăți către panou: contractul în 5 câmpuri nu
are câmp de secvență, și nu știu ce `actiune` așteaptă panoul tău pentru indicatorul
de „scrie acum". Spune-mi numele acțiunii și îl adaug.
