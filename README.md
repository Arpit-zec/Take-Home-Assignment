# Ecommerce Cohort Insights API

A take-home backend for processing ecommerce product descriptions, campaign notes, and customer feedback into mock summaries and keyword tags. Built with Python 3.11+, FastAPI, MongoDB, and Redis. No AI service or API key is needed.

## Run

```bash
docker compose up --build -d
```

- API docs: http://localhost:8000/docs
- Health: http://localhost:8000/health
- MongoDB: `localhost:27018`; Redis: `localhost:6380` (local access only).
- Logs: `docker compose logs -f api worker`
- Stop: `docker compose down` (keeps data).

Compose starts the API, a separate worker, MongoDB, and Redis. The worker handles two stages concurrently by default. Initial image downloads can take a few minutes. Database host ports can be changed with `MONGO_PORT` and `REDIS_PORT`; update local/test URLs accordingly.

For local development:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
cp .env.example .env
docker compose up -d mongo redis
uvicorn app.main:app --reload
# Separate terminal, same virtual environment:
python -m app.worker
```

If the full Compose stack is already running, stop its API and worker before starting local versions: `docker compose stop api worker`.

## Try it

`X-User-Id` represents the authenticated merchant in this POC. The body `user_id` must match it. This is a trusted identity stub, **not production authentication**: deploy behind a gateway that validates tokens and sets this identity, or replace the dependency with JWT verification. A partner must use the submitting merchant's identity too.

```bash
curl -X POST http://localhost:8000/documents \
  -H 'Content-Type: application/json' -H 'X-User-Id: merchant-1' \
  -d '{"user_id":"merchant-1","title":"Trail running shoes","content":"Lightweight trail shoes with breathable mesh and grippy rubber soles.","client_doc_ref":"catalog:sku-101"}'

curl http://localhost:8000/documents/by-ref/catalog:sku-101 \
  -H 'X-User-Id: merchant-1'

curl 'http://localhost:8000/users/merchant-1/documents?page=1&page_size=10&status=completed' \
  -H 'X-User-Id: merchant-1'
```

Use the returned `document_id` and `version` for updates:

```bash
curl -X PATCH http://localhost:8000/documents/DOCUMENT_ID \
  -H 'Content-Type: application/json' -H 'X-User-Id: merchant-1' \
  -d '{"content":"Waterproof trail shoes with reinforced toe protection.","expected_version":1}'
```

`expected_version` is optional. Send it to get a 409 when someone else updated the document first; omit it to accept last-writer-wins against whatever version is current.

| Endpoint | Behavior |
| --- | --- |
| `POST /documents` | 201 for a new document; 200 for an identical external-ref retry |
| `PATCH /documents/{id}` | 200; always restarts both stages; optional `expected_version` |
| `GET /documents/{id}` | Current content, stage progress, and versioned results |
| `GET /users/{user_id}/documents` | Newest first; page ≥ 1; page size 1–100; optional status |
| `GET /documents/by-ref/{ref}` | Indexed lookup, scoped to the owner |
| `GET /health` | MongoDB and Redis connectivity; 200 healthy or 503 degraded |

Unknown and foreign documents both return 404. Foreign user listings return 404. Conflicting versions/references return 409, full active capacity returns 429, invalid inputs return 422, and dependency outages return 503. Titles and content are trimmed and bounded; refs accept letters, numbers, `_`, `.`, `:`, and `-`.

## Pipeline and recovery

MongoDB is the durable queue: inserting a queued document also schedules it, avoiding a separate enqueue write. Synchronous database calls run in FastAPI's thread pool; processing runs in the independent worker process, never inside the request.

`queued → processing → enriching → completed | failed`

Processing waits 10–20 seconds and produces a short mock summary. Enrichment waits 5–15 seconds and extracts up to five keywords **from that summary**. Each attempt has an independent 10% failure probability. Both stages use the same structured fields: `status`, `attempts`, `version`, `error`, and `retry_at`.

A failed attempt retries after 2, then 4 seconds, with three attempts maximum. During backoff the overall status remains active and the stage reports `failed` with `retry_at`. Exhaustion sets the overall status to `failed`. Enrichment retries retain the successful summary and never rerun processing. A later PATCH deliberately starts both stages again.

Workers atomically claim one document with a random lease token and a 90-second lease. All stage writes require the token and content version. A crashed worker's job becomes available after lease expiry; an obsolete worker cannot overwrite a newer claim. Simulations are bounded to 20 seconds, so lease renewal is unnecessary here. Shutdown interrupts waits; unfinished work remains durable.

## Schema & Staleness Design

Each document stores its content, SHA-256 hash, integer `version`, both stage records, and optional `summary`/`tags`. Each derived field carries **both** its producing version and content hash.

PATCH uses a single atomic MongoDB update matching the expected version: the value the caller sent, or the current version read under the per-user lock when it is omitted. It increments the version, replaces the content/hash, clears both derived fields, resets both stages, and invalidates the worker lease together. Two PATCHes expecting version 1 cannot both succeed; the loser gets 409. Two PATCHes that omit the field serialize on the lock and produce versions 2 and 3, never a merged document.

Stage writes match `_id + version + lease_token`. A version-1 worker finishing after a version-2 PATCH matches nothing. Reads obtain one MongoDB document snapshot and expose a derived field only when its version/hash match the current content. Tags are also hidden without a matching summary. Therefore a reader sees the old snapshot before PATCH, or the new snapshot after it, never a combination of their fields. A current partial summary can appear during enrichment, but `result_current` becomes true only when both results are complete. Old results are removed rather than displayed as stale.

## References, cache, and capacity

**External references:** globally unique when present. Same owner, ref, title, and normalized content returns the existing document without new work. Different content/title or another owner receives a generic 409; refs never silently move between documents. Out-of-order conflicting submissions are rejected. PATCH keeps the mapping; retrying the original POST after a content change returns 409. Without a ref, each POST creates a new document.

**Caching:** `insights:v1:{user_id}:{sha256(content)}` stores only completed results for 24 hours. The `v1` namespace identifies the mock algorithm. Cache hits create a completed document immediately and do not consume capacity. Results are rebound to the new document's version. PATCH always reprocesses; the old hash's cache remains valid for that old content. Whitespace at the edges is trimmed; internal whitespace and case remain significant. Cache errors are logged and treated as misses.

**Capacity:** Redis holds `active:{user_id}`, counting queued, processing, enriching, and retry backoff. It is the admission gate, not a mirror of MongoDB: a merchant already at three is rejected on a single Redis read, with no database query at all, which is what keeps a submit flood cheap. Admission and terminal transitions apply `INCRBY` under a per-user Redis lock. The counter carries a 120-second TTL and is rebuilt from MongoDB only when it is missing, so an eviction or a Redis restart cannot permanently leak capacity. The lock expires after 60 seconds; database calls time out after five seconds, and no simulated work runs inside the lock.

As a small durability safeguard, active documents also occupy slot 0, 1, or 2, with a unique MongoDB `(user_id, active_slot)` index. Completion/failure atomically removes the slot. Even if Redis restarts or a process outlives its lock, concurrent admissions cannot create a fourth slot. A slot collision returns 429 and can be retried. Redis outages fail writes closed with 503; reads still work, and workers resume after recovery. Redis uses AOF and `noeviction`.

Indexes created at startup:

- `(user_id, created_at DESC, _id DESC)` for listing.
- `(user_id, status, created_at DESC, _id DESC)` for filtered lists/counts.
- Unique partial `client_doc_ref_1` for string refs; the by-ref route explicitly hints it, even for tiny collections.
- `(user_id, content_hash)` for content queries.
- `(status, available_at, lease_until)` for runnable jobs.
- Unique partial `(user_id, active_slot)` for integer active slots.

## Tests and quality checks

```bash
source .venv/bin/activate
docker compose up -d mongo redis
make check    # Ruff, Black --check, strict mypy
make test     # Unit + real MongoDB/Redis integration tests
# Unit tests only:
pytest -q -m 'not integration'
```

Tests use a separate randomly named MongoDB database and Redis database 15, removing only their own data. Override `TEST_MONGO_URL` / `TEST_REDIS_URL` if needed. Missing services fail integration tests rather than silently skipping them. Tests cover caching, ownership, external-ref conflicts, both failure stages, retries, pagination, admission races, competing PATCHes, updates during both stages, lease recovery, index use, dependency failures, and HTTP clients.

## At 100×

A merchant with 500K documents creates a large contiguous range in the user-leading indexes. Equality on `user_id` still narrows the search, but counting all matches and walking deep pages become expensive. A frequently queried merchant can consume a disproportionate share of index-cache memory and disk I/O. The status-leading suffix helps active counts, which should contain at most three records, but it does not make a completed-document count cheap. I would inspect query plans, cache approximate totals where acceptable, and measure examined keys versus returned rows before adding more indexes.

For horizontal document storage, I would use `(user_id, bucket)`, where `bucket` is a stable hash of the document ID modulo a fixed bucket count. This distributes a large merchant across partitions; plain `user_id`, including hashed `user_id`, leaves all of that merchant's records together. The tradeoff is that user-wide lists must query several buckets and merge their ordered results. Point reads can derive the bucket from the document ID. A global external-reference constraint would move into a separate crosswalk collection partitioned by hashed reference, with a unique reference index and an explicit transactional or reservation protocol. The current global unique index cannot simply be carried over to an unrelated MongoDB shard key. The three active slots would similarly move to a small admission collection whose partition key includes the merchant, preserving the per-user uniqueness invariant.

At 100× submit QPS, the counter itself holds up: a rejection costs one Redis `GET` and never reaches MongoDB, and an admission adds one `INCRBY`. The per-user lock is the real bottleneck, since every admitted submit takes and releases it around a MongoDB write, so one merchant's burst serializes into a lock queue even though the policy allows only three jobs. The `GET`, the slot read, and the `INCRBY` are also separate round trips, so the counter can disagree with MongoDB in the window between them. I would collapse admission into one atomic Lua reservation keyed by merchant with idempotent job tokens, dropping the lock entirely, and reconcile reservations through a durable outbox. Redis Cluster keys participating in one script need the same hash tag. The three-job policy means a busy merchant should be rejected quickly rather than generate a lock queue. Cross-store failure handling and reservation expiry would need fault-injection tests before replacing the current database safeguard.

Finally, skip/limit scans and discards the offset, so page 40,000 is inherently costly. I would offer cursor pagination using `(created_at, _id)` as the stable descending boundary, with the user and filter encoded in a signed cursor. Each bucket returns only the next bounded page. New inserts would no longer shift previously traversed offsets. Exact totals would become optional, and worker queue lag, admission latency, and cache hit rate would guide scaling decisions.

## With more time

Add verified authentication, a reproducible transitive dependency lock and security-update workflow, request IDs and metrics, and an operational dead-letter/retry interface. Move long-running real integrations to a dedicated queue with renewable leases and fair scheduling. Add crash/failover testing and deployment migrations. A timeout after a successful write can still return 503: callers should use `client_doc_ref` to retry POST safely and reload the version before retrying PATCH. Listing totals and items are separate reads, so concurrent writes can change the total between them.

The repository is ready for review; publishing it to GitHub and emailing its link are submission steps outside this local implementation.
