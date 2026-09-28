from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from unittest.mock import patch

import httpx
import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.main import create_app
from app.models import ContentUpdate, Submission
from app.service import DocumentService, utcnow
from app.worker import Worker

pytestmark = pytest.mark.integration


def test_pipeline_cache_and_patch(client, storage, user, submit):
    headers = {"X-User-Id": user}
    created = submit()
    assert created.status_code == 201
    doc_id = created.json()["document_id"]
    worker = Worker(storage)
    assert worker.run_one()
    partial = client.get(f"/documents/{doc_id}", headers=headers).json()
    assert partial["status"] == "enriching"
    assert partial["summary"]["version"] == 1 and partial["tags"] is None
    assert not partial["result_current"]
    assert worker.run_one()
    complete = client.get(f"/documents/{doc_id}", headers=headers).json()
    assert complete["result_current"] and complete["tags"]["version"] == 1
    cached = submit().json()
    assert cached["status"] == "completed"
    assert cached["processing"]["attempts"] == 0
    updated = client.patch(
        f"/documents/{doc_id}",
        headers=headers,
        json={"content": "Waterproof hiking backpack", "expected_version": 1},
    )
    assert updated.status_code == 200
    assert updated.json()["version"] == 2
    assert updated.json()["summary"] is None and updated.json()["tags"] is None
    assert not updated.json()["result_current"]
    worker.run_one()
    worker.run_one()
    new = client.get(f"/documents/{doc_id}", headers=headers).json()
    assert new["summary"]["version"] == new["tags"]["version"] == 2
    assert "backpack" in new["tags"]["values"]
    assert submit().json()["summary"]["text"] == complete["summary"]["text"]


def test_crosswalk_and_ownership(client, user, submit):
    first = submit(ref="sku-123")
    assert first.status_code == 201
    assert submit(ref="sku-123").status_code == 200
    assert submit(content="Different", ref="sku-123").status_code == 409
    doc_id = first.json()["document_id"]
    for path in (
        f"/documents/{doc_id}",
        "/documents/by-ref/sku-123",
        f"/users/{user}/documents",
    ):
        assert client.get(path, headers={"X-User-Id": "other"}).status_code == 404
    assert (
        client.get("/documents/by-ref/sku-123", headers={"X-User-Id": user}).json()[
            "document_id"
        ]
        == doc_id
    )
    assert (
        client.get("/documents/invalid", headers={"X-User-Id": user}).status_code == 404
    )
    assert (
        client.patch(
            f"/documents/{doc_id}",
            headers={"X-User-Id": "other"},
            json={"content": "x", "expected_version": 1},
        ).status_code
        == 404
    )
    assert (
        client.post(
            "/documents",
            headers={"X-User-Id": "other"},
            json={"user_id": user, "title": "x", "content": "x"},
        ).status_code
        == 403
    )
    assert client.get(f"/documents/{doc_id}").status_code == 422


def test_limit_counts_both_stages_and_recovers(client, storage, user, submit):
    for index in range(3):
        assert submit(content=f"Shoes {index}").status_code == 201
    assert submit(content="fourth").status_code == 429
    worker = Worker(storage)
    worker.run_one()
    assert submit(content="still full").status_code == 429
    # Count cache eviction is recovered from durable active records.
    storage.redis.delete(f"active:{user}")
    assert submit(content="after eviction").status_code == 429
    for _ in range(5):
        worker.run_one()
    assert submit(content="now allowed").status_code == 201


def test_enrichment_failure_keeps_summary(storage, user, submit):
    doc_id = submit().json()["document_id"]
    worker = Worker(storage)
    worker.run_one()
    storage.settings.failure_probability = 1
    storage.settings.max_stage_attempts = 2
    worker.run_one()
    service = DocumentService(storage)
    failed = service.get(doc_id, user)
    assert failed["enriching"]["status"] == "failed"
    assert failed["enriching"]["retry_at"] is not None
    assert failed["processing"]["attempts"] == 1
    summary = failed["summary"]
    storage.documents.update_one(
        {"_id": failed["_id"]}, {"$set": {"available_at": utcnow()}}
    )
    worker.run_one()
    exhausted = service.get(doc_id, user)
    assert exhausted["status"] == "failed"
    assert exhausted["summary"] == summary
    assert exhausted["enriching"]["attempts"] == 2
    assert exhausted["processing"]["attempts"] == 1
    assert int(storage.redis.get(f"active:{user}")) == 0


def test_patch_fences_inflight_stage(storage, user, submit):
    doc_id = submit().json()["document_id"]
    worker = Worker(storage)
    service = DocumentService(storage)
    original_wait = worker.stop.wait

    def update_during_work(timeout):
        service.update(
            doc_id, user, ContentUpdate(content="New backpack", expected_version=1)
        )
        return False

    with patch.object(worker.stop, "wait", side_effect=update_during_work):
        worker.run_one()
    assert worker.stop.wait == original_wait
    current = service.get(doc_id, user)
    assert current["version"] == 2 and current["summary"] is None
    assert current["status"] == "queued"
    worker.run_one()
    worker.run_one()
    assert service.get(doc_id, user)["tags"]["version"] == 2


def test_competing_patches_and_submissions(storage, user):
    service = DocumentService(storage)

    def submit(index):
        try:
            return service.submit(
                Submission(user_id=user, title="Shoes", content=f"Cotton {index}")
            )[0]
        except Exception as exc:
            return exc

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(submit, range(8)))
    docs = [result for result in results if isinstance(result, dict)]
    assert len(docs) == 3
    assert all(
        getattr(result, "status_code", None) == 429
        for result in results
        if not isinstance(result, dict)
    )
    doc_id = str(docs[0]["_id"])

    def update(index):
        try:
            return service.update(
                doc_id,
                user,
                ContentUpdate(content=f"Updated {index}", expected_version=1),
            )["version"]
        except Exception as exc:
            return getattr(exc, "status_code", None)

    with ThreadPoolExecutor(max_workers=2) as pool:
        assert sorted(pool.map(update, range(2))) == [2, 409]


def test_claim_recovery_and_fencing(storage, user, submit):
    doc_id = submit().json()["document_id"]
    worker = Worker(storage)
    first = worker.claim()
    assert first and worker.claim() is None
    storage.documents.update_one(
        {"_id": first["_id"]},
        {"$set": {"lease_until": utcnow() - timedelta(seconds=1)}},
    )
    second = worker.claim()
    assert second["lease_token"] != first["lease_token"]
    stale = storage.documents.update_one(
        {"_id": first["_id"], "version": 1, "lease_token": first["lease_token"]},
        {"$set": {"status": "completed"}},
    )
    assert stale.matched_count == 0
    assert DocumentService(storage).get(doc_id, user)["status"] == "queued"


def test_pagination_validation_and_health(client, user, submit):
    for index in range(3):
        submit(content=f"Item {index}")
    headers = {"X-User-Id": user}
    page = client.get(
        f"/users/{user}/documents?page=1&page_size=2&status=queued", headers=headers
    ).json()
    assert page["total"] == 3 and len(page["items"]) == 2
    assert page["items"][0]["content"] == "Item 2"
    second = client.get(
        f"/users/{user}/documents?page=2&page_size=2", headers=headers
    ).json()
    assert len(second["items"]) == 1
    assert (
        client.get(f"/users/{user}/documents?page=0", headers=headers).status_code
        == 422
    )
    assert client.get("/health").status_code == 200


def test_redis_outage_read_available_write_fails_closed(client, storage, user, submit):
    doc_id = submit().json()["document_id"]
    with patch.object(storage.redis, "ping", side_effect=RedisConnectionError):
        assert client.get("/health").status_code == 503
    with patch.object(storage.redis, "lock", side_effect=RedisConnectionError):
        assert submit(content="Another item").status_code == 503
        assert (
            client.get(f"/documents/{doc_id}", headers={"X-User-Id": user}).status_code
            == 200
        )
    with patch.object(storage.redis, "get", side_effect=RedisConnectionError):
        assert storage.cached(user, "hash") is None


def test_by_ref_uses_unique_index(storage, user, submit):
    submit(ref="sku-index")
    indexes = storage.documents.index_information()
    assert indexes["client_doc_ref_1"]["unique"]
    plan = (
        storage.documents.find({"client_doc_ref": "sku-index"})
        .hint("client_doc_ref_1")
        .explain()
    )
    assert "IXSCAN" in str(plan["queryPlanner"]["winningPlan"])


def test_async_http_client(storage, user):
    import asyncio

    async def run():
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=create_app(storage)),
            base_url="http://test",
        ) as client:
            response = await client.post(
                "/documents",
                headers={"X-User-Id": user},
                json={"user_id": user, "title": "Bag", "content": "Leather tote bag"},
            )
            assert response.status_code == 201

    asyncio.run(run())


def test_database_guard_survives_lost_redis_lock(storage, user):
    from contextlib import nullcontext

    from fastapi import HTTPException

    service = DocumentService(storage)

    def create(index):
        try:
            service.submit(
                Submission(user_id=user, title="Bag", content=f"Bag {index}")
            )
            return 201
        except HTTPException as exc:
            return exc.status_code

    # Force all requests to choose slot zero, as if their locks were lost.
    with (
        patch.object(storage, "user_lock", return_value=nullcontext()),
        patch.object(storage, "check_capacity", return_value=0),
    ):
        with ThreadPoolExecutor(max_workers=6) as pool:
            results = list(pool.map(create, range(6)))
    assert results.count(201) == 1
    assert results.count(429) == 5


def test_processing_failure_and_successful_enrichment_retry(storage, user, submit):
    first_id = submit().json()["document_id"]
    worker = Worker(storage)
    storage.settings.failure_probability = 1
    storage.settings.max_stage_attempts = 1
    worker.run_one()
    failed = DocumentService(storage).get(first_id, user)
    assert failed["status"] == "failed"
    assert failed["processing"]["status"] == "failed"
    assert failed["enriching"]["attempts"] == 0
    assert failed["summary"] is None
    storage.settings.failure_probability = 0
    storage.settings.max_stage_attempts = 3
    second_id = submit(content="Wool winter jacket").json()["document_id"]
    worker.run_one()
    storage.settings.failure_probability = 1
    worker.run_one()
    second = DocumentService(storage).get(second_id, user)
    storage.documents.update_one(
        {"_id": second["_id"]}, {"$set": {"available_at": utcnow()}}
    )
    storage.settings.failure_probability = 0
    worker.run_one()
    done = DocumentService(storage).get(second_id, user)
    assert done["status"] == "completed"
    assert done["processing"]["attempts"] == 1
    assert done["enriching"]["attempts"] == 2


def test_patch_at_capacity_and_old_ref_retry(client, storage, user, submit):
    original = submit(ref="stable-sku").json()
    worker = Worker(storage)
    worker.run_one()
    worker.run_one()
    for index in range(3):
        submit(content=f"Active {index}")
    headers = {"X-User-Id": user}
    path = f"/documents/{original['document_id']}"
    assert (
        client.patch(
            path, headers=headers, json={"content": "New", "expected_version": 1}
        ).status_code
        == 429
    )
    # Cache hits do not consume a fourth active slot.
    assert submit().json()["status"] == "completed"
    for _ in range(6):
        worker.run_one()
    assert (
        client.patch(
            path, headers=headers, json={"content": "New", "expected_version": 1}
        ).status_code
        == 200
    )
    assert submit(ref="stable-sku").status_code == 409


def test_enrichment_racing_patch_cannot_publish_old_tags(storage, user, submit):
    doc_id = submit().json()["document_id"]
    worker = Worker(storage)
    worker.run_one()
    service = DocumentService(storage)

    def patch_content(timeout):
        service.update(
            doc_id, user, ContentUpdate(content="New hiking boots", expected_version=1)
        )
        return False

    with patch.object(worker.stop, "wait", side_effect=patch_content):
        worker.run_one()
    doc = service.get(doc_id, user)
    assert doc["version"] == 2
    assert doc["summary"] is None and doc["tags"] is None
