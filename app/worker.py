import logging
import random
import re
import signal
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Any
from uuid import uuid4

from pymongo import ReturnDocument
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.config import Settings
from app.logging_config import configure_logging
from app.service import utcnow
from app.storage import ACTIVE, Record, Storage

logger = logging.getLogger(__name__)


def summarize(content: str) -> str:
    words = content.split()
    return (
        "Product insights: " + " ".join(words[:60]) + ("…" if len(words) > 60 else "")
    )


def keywords(summary: str) -> list[str]:
    stopwords = {
        "product",
        "insights",
        "with",
        "this",
        "that",
        "from",
        "have",
        "your",
        "and",
        "the",
        "for",
    }
    words = [
        word
        for word in re.findall(r"[a-z]{3,}", summary.lower())
        if word not in stopwords
    ]
    return [word for word, _ in Counter(words).most_common(5)]


class Worker:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.settings = storage.settings
        self.stop = threading.Event()

    def claim(self) -> Record | None:
        now = utcnow()
        return self.storage.documents.find_one_and_update(
            {
                "status": {"$in": ACTIVE},
                "available_at": {"$lte": now},
                "lease_until": {"$lte": now},
            },
            {
                "$set": {
                    "lease_token": uuid4().hex,
                    "lease_until": now + timedelta(seconds=90),
                }
            },
            sort=[("available_at", 1)],
            return_document=ReturnDocument.AFTER,
        )

    def run_one(self) -> bool:
        document = self.claim()
        if document is None:
            return False
        stage = (
            "enriching"
            if document["processing"]["status"] == "completed"
            else "processing"
        )
        query = {
            "_id": document["_id"],
            "version": document["version"],
            "lease_token": document["lease_token"],
        }
        attempt = document[stage]["attempts"] + 1
        started = self.storage.documents.update_one(
            query,
            {
                "$set": {
                    "status": stage,
                    f"{stage}.status": "running",
                    f"{stage}.attempts": attempt,
                    f"{stage}.error": None,
                    f"{stage}.retry_at": None,
                    "updated_at": utcnow(),
                }
            },
        )
        if not started.matched_count:
            return True
        logger.info(
            "stage_started",
            extra={
                "document_id": str(document["_id"]),
                "stage": stage,
                "version": document["version"],
            },
        )
        duration = (
            random.uniform(10, 20) if stage == "processing" else random.uniform(5, 15)
        )
        if self.stop.wait(duration * self.settings.simulation_scale):
            return True  # Lease expiry makes interrupted work claimable again.
        failed = random.random() < self.settings.failure_probability
        fields: Record = {
            "lease_until": utcnow(),
            "lease_token": None,
            "updated_at": utcnow(),
            "available_at": utcnow(),
        }
        if failed:
            exhausted = attempt >= self.settings.max_stage_attempts
            retry_at = None if exhausted else utcnow() + timedelta(seconds=2**attempt)
            fields.update(
                {
                    "status": "failed" if exhausted else stage,
                    f"{stage}.status": "failed",
                    f"{stage}.error": "Simulated stage failure",
                    f"{stage}.retry_at": retry_at,
                    "available_at": retry_at or utcnow(),
                }
            )
        else:
            fields.update(
                {
                    f"{stage}.status": "completed",
                    f"{stage}.version": document["version"],
                    f"{stage}.error": None,
                    f"{stage}.retry_at": None,
                }
            )
            derived = {
                "version": document["version"],
                "content_hash": document["content_hash"],
            }
            if stage == "processing":
                fields.update(
                    status="enriching",
                    summary={**derived, "text": summarize(document["content"])},
                )
            else:
                fields.update(
                    status="completed",
                    tags={**derived, "values": keywords(document["summary"]["text"])},
                )
        # Same per-user lock as admission: terminal transitions update the count safely.
        changes: Record = {"$set": fields}
        if fields["status"] in ("completed", "failed"):
            changes["$unset"] = {"active_slot": ""}
        with self.storage.user_lock(document["user_id"]):
            updated = self.storage.documents.find_one_and_update(
                query,
                changes,
                return_document=ReturnDocument.AFTER,
            )
            if updated:
                if updated["status"] not in ACTIVE:
                    self.storage.adjust_count(document["user_id"], -1)
                if updated["status"] == "completed":
                    self.storage.cache_result(updated)
                logger.info(
                    "stage_finished",
                    extra={
                        "document_id": str(document["_id"]),
                        "stage": stage,
                        "version": document["version"],
                        "status": updated["status"],
                    },
                )
        return True

    def loop(self) -> None:
        while not self.stop.is_set():
            try:
                if not self.run_one():
                    self.stop.wait(0.5)
            except (PyMongoError, RedisError):
                logger.exception("worker_dependency_unavailable")
                self.stop.wait(2)
            except Exception:
                logger.exception("worker_unexpected_error")
                self.stop.wait(2)


def main() -> None:
    configure_logging()
    storage = Storage(Settings())
    storage.indexes()
    worker = Worker(storage)

    def shutdown(signum: int, frame: Any) -> None:
        worker.stop.set()

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        with ThreadPoolExecutor(
            max_workers=storage.settings.worker_concurrency
        ) as pool:
            futures = [
                pool.submit(worker.loop)
                for _ in range(storage.settings.worker_concurrency)
            ]
            for future in futures:
                future.result()
    finally:
        storage.close()


if __name__ == "__main__":
    main()
