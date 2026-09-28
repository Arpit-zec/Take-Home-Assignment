import hashlib
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from redis import Redis
from redis.exceptions import RedisError

from app.config import Settings

ACTIVE = ["queued", "processing", "enriching"]
MAX_ACTIVE = 3
COUNT_TTL = 120
logger = logging.getLogger(__name__)
Record = dict[str, Any]


def content_hash(content: str) -> str:
    return hashlib.sha256(content.encode()).hexdigest()


class Storage:
    def __init__(self, settings: Settings):
        self.settings = settings
        self.mongo: MongoClient[Record] = MongoClient(
            settings.mongo_url, timeoutMS=5000, tz_aware=True
        )
        self.documents: Collection[Record] = self.mongo[
            settings.mongo_database
        ].documents
        self.redis: Redis = Redis.from_url(
            settings.redis_url,
            decode_responses=True,
            socket_timeout=2,
            socket_connect_timeout=2,
        )

    def indexes(self) -> None:
        self.documents.create_index(
            [("user_id", ASCENDING), ("created_at", DESCENDING), ("_id", DESCENDING)]
        )
        self.documents.create_index(
            [
                ("user_id", ASCENDING),
                ("status", ASCENDING),
                ("created_at", DESCENDING),
                ("_id", DESCENDING),
            ]
        )
        self.documents.create_index(
            "client_doc_ref",
            unique=True,
            partialFilterExpression={"client_doc_ref": {"$type": "string"}},
        )
        self.documents.create_index([("user_id", 1), ("content_hash", 1)])
        # Backstop the Redis lock if it expires or Redis restarts mid-request.
        self.documents.create_index(
            [("user_id", 1), ("active_slot", 1)],
            unique=True,
            partialFilterExpression={"active_slot": {"$type": "int"}},
        )
        self.documents.create_index(
            [("status", 1), ("available_at", 1), ("lease_until", 1)]
        )

    def close(self) -> None:
        self.mongo.close()
        self.redis.close()

    @contextmanager
    def user_lock(self, user_id: str) -> Iterator[None]:
        # Never hold this lock while simulating work. Mongo calls have a 5s timeout.
        with self.redis.lock(f"admission:{user_id}", timeout=60, blocking_timeout=5):
            yield

    def refresh_count(self, user_id: str) -> int:
        # Rebuild from the durable records after an eviction or a Redis restart.
        count = self.documents.count_documents(
            {"user_id": user_id, "status": {"$in": ACTIVE}}
        )
        self.redis.set(f"active:{user_id}", count, ex=COUNT_TTL)
        return count

    def active_count(self, user_id: str) -> int:
        value = cast(str | None, self.redis.get(f"active:{user_id}"))
        if value is None:
            return self.refresh_count(user_id)
        return max(0, int(value))

    def adjust_count(self, user_id: str, delta: int) -> None:
        # Callers hold the per-user lock, so these stay ordered. The TTL bounds
        # any drift left by a crash between the Mongo write and this update.
        pipeline = self.redis.pipeline()
        pipeline.incrby(f"active:{user_id}", delta)
        pipeline.expire(f"active:{user_id}", COUNT_TTL)
        pipeline.execute()

    def next_slot(self, user_id: str) -> int | None:
        # One Redis read turns away a user who is already full, so a merchant
        # spamming submits never reaches MongoDB. The unique (user_id,
        # active_slot) index is what actually enforces the limit.
        if self.active_count(user_id) >= MAX_ACTIVE:
            return None
        occupied = {
            document["active_slot"]
            for document in self.documents.find(
                {"user_id": user_id, "active_slot": {"$exists": True}},
                {"active_slot": 1},
            )
        }
        for slot in range(MAX_ACTIVE):
            if slot not in occupied:
                return slot
        self.refresh_count(user_id)  # Counter ran behind; resync from MongoDB.
        return None

    def cached(self, user_id: str, digest: str) -> Record | None:
        try:
            raw = cast(str | None, self.redis.get(f"insights:v1:{user_id}:{digest}"))
            if raw:
                value = json.loads(raw)
                if (
                    isinstance(value, dict)
                    and isinstance(value.get("summary"), str)
                    and isinstance(value.get("tags"), list)
                    and all(isinstance(tag, str) for tag in value["tags"])
                ):
                    return value
        except (RedisError, ValueError, TypeError):
            logger.warning("cache_read_failed", exc_info=True)
        return None

    def cache_result(self, document: Record) -> None:
        try:
            self.redis.set(
                f"insights:v1:{document['user_id']}:{document['content_hash']}",
                json.dumps(
                    {
                        "summary": document["summary"]["text"],
                        "tags": document["tags"]["values"],
                    }
                ),
                ex=self.settings.cache_ttl_seconds,
            )
        except RedisError:
            logger.warning("cache_write_failed", exc_info=True)
