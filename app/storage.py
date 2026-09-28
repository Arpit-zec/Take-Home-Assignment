import hashlib
import json
import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, cast

from fastapi import HTTPException
from pymongo import ASCENDING, DESCENDING, MongoClient
from pymongo.collection import Collection
from redis import Redis
from redis.exceptions import RedisError

from app.config import Settings

ACTIVE = ["queued", "processing", "enriching"]
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
        count = self.documents.count_documents(
            {"user_id": user_id, "status": {"$in": ACTIVE}}
        )
        self.redis.set(f"active:{user_id}", count, ex=120)
        return count

    def check_capacity(self, user_id: str) -> int:
        # Rebuild under the user lock: crashes/Redis restarts cannot leak a slot.
        self.refresh_count(user_id)
        count = int(cast(str | None, self.redis.get(f"active:{user_id}")) or 0)
        if count >= 3:
            raise HTTPException(
                429,
                "At most 3 active documents per user",
                headers={"Retry-After": "10"},
            )

        occupied = {
            document["active_slot"]
            for document in self.documents.find(
                {"user_id": user_id, "active_slot": {"$exists": True}},
                {"active_slot": 1},
            )
        }
        for slot in range(3):
            if slot not in occupied:
                return slot
        raise HTTPException(429, "At most 3 active documents per user")

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
