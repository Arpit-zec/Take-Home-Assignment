import os
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from pymongo.errors import PyMongoError
from redis.exceptions import RedisError

from app.config import Settings
from app.main import create_app
from app.storage import Storage


@pytest.fixture
def storage():
    settings = Settings(
        mongo_url=os.getenv("TEST_MONGO_URL", "mongodb://localhost:27018"),
        mongo_database=f"test_insights_{uuid4().hex}",
        redis_url=os.getenv("TEST_REDIS_URL", "redis://localhost:6380/15"),
        simulation_speed=0,
        failure_probability=0,
    )
    store = Storage(settings)
    try:
        store.mongo.admin.command("ping")
        store.redis.ping()
    except (PyMongoError, RedisError) as exc:
        store.close()
        pytest.fail(
            "Integration services unavailable. Run docker compose up -d mongo redis. "
            f"Details: {exc}"
        )
    store.indexes()
    yield store
    # Remove only keys for this fixture's user, never flush a shared Redis database.
    users = store.documents.distinct("user_id")
    for user in users:
        keys = list(store.redis.scan_iter(match=f"*:{user}*"))
        if keys:
            store.redis.delete(*keys)
    store.mongo.drop_database(settings.mongo_database)
    store.close()


@pytest.fixture
def user():
    return f"shop_{uuid4().hex}"


@pytest.fixture
def client(storage):
    with TestClient(create_app(storage)) as test_client:
        yield test_client


@pytest.fixture
def submit(client, user):
    def create(content="Lightweight cotton running shoes with rubber soles", ref=None):
        return client.post(
            "/documents",
            headers={"X-User-Id": user},
            json={
                "user_id": user,
                "title": "Running shoes",
                "content": content,
                "client_doc_ref": ref,
            },
        )

    return create
