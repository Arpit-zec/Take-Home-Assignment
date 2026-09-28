import pytest
from bson import ObjectId
from pydantic import ValidationError

from app.models import Submission
from app.service import pipeline_fields, utcnow, view
from app.storage import content_hash
from app.worker import keywords, summarize


def test_input_validation():
    for content in ("", "   ", "x" * 100001):
        with pytest.raises(ValidationError):
            Submission(user_id="shop", title="Shoes", content=content)
    with pytest.raises(ValidationError):
        Submission(
            user_id="shop", title="Shoes", content="Cotton", client_doc_ref="bad/ref"
        )


def test_read_guard_hides_mixed_versions():
    record = {
        "_id": ObjectId(),
        "user_id": "shop",
        "title": "Shoes",
        "created_at": utcnow(),
        **pipeline_fields("New content", 2),
    }
    record.update(
        status="completed",
        summary={"version": 1, "content_hash": content_hash("Old"), "text": "Old"},
        tags={"version": 2, "content_hash": record["content_hash"], "values": ["new"]},
    )
    result = view(record)
    assert result.summary is None and result.tags is None
    assert not result.result_current


def test_hash_and_mock_insights():
    assert content_hash("cotton") != content_hash("Cotton")
    summary = summarize("Cotton shoes cotton lightweight comfortable")
    assert "Cotton shoes" in summary
    assert keywords(summary)[0] == "cotton"
