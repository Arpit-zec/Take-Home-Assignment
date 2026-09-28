from datetime import UTC, datetime
from typing import Any

from bson import ObjectId
from fastapi import HTTPException
from pymongo import ReturnDocument
from pymongo.errors import DuplicateKeyError

from app.models import ContentUpdate, DocumentView, Stage, Submission
from app.storage import ACTIVE, MAX_ACTIVE, Record, Storage, content_hash


def utcnow() -> datetime:
    return datetime.now(UTC)


def object_id(value: str) -> ObjectId:
    if not ObjectId.is_valid(value):
        raise HTTPException(404, "Document not found")
    return ObjectId(value)


def view(document: Record) -> DocumentView:
    # Read all fields from ONE Mongo snapshot; only expose matching derived data.
    data = dict(document)
    version, digest = data["version"], data["content_hash"]
    for field in ("summary", "tags"):
        result = data.get(field)
        if result and (
            result["version"] != version or result["content_hash"] != digest
        ):
            data[field] = None
    if not data.get("summary"):
        data["tags"] = None
    data["result_current"] = bool(
        data["status"] == "completed" and data.get("summary") and data.get("tags")
    )
    data["document_id"] = str(data.pop("_id"))
    return DocumentView.model_validate(data)


def pipeline_fields(content: str, version: int, cached: Record | None = None) -> Record:
    digest = content_hash(content)
    fields: Record = {
        "content": content,
        "content_hash": digest,
        "version": version,
        "status": "queued",
        "processing": Stage().model_dump(),
        "enriching": Stage().model_dump(),
        "summary": None,
        "tags": None,
        "lease_token": None,
        "lease_until": utcnow(),
        "available_at": utcnow(),
        "updated_at": utcnow(),
    }
    if cached:
        fields.update(
            status="completed",
            processing=Stage(status="completed", version=version).model_dump(),
            enriching=Stage(status="completed", version=version).model_dump(),
            summary={
                "version": version,
                "content_hash": digest,
                "text": cached["summary"],
            },
            tags={"version": version, "content_hash": digest, "values": cached["tags"]},
        )
    return fields


class DocumentService:
    def __init__(self, storage: Storage):
        self.storage = storage
        self.documents = storage.documents

    def claim_slot(self, user_id: str) -> int:
        slot = self.storage.next_slot(user_id)
        if slot is None:
            raise HTTPException(
                429,
                f"At most {MAX_ACTIVE} active documents per user",
                headers={"Retry-After": "10"},
            )
        return slot

    def get(self, document_id: str, user_id: str) -> Record:
        document = self.documents.find_one(
            {"_id": object_id(document_id), "user_id": user_id}
        )
        if document is None:
            raise HTTPException(404, "Document not found")
        return document

    def by_ref(self, ref: str, user_id: str) -> Record:
        document = self.documents.find_one(
            {"client_doc_ref": ref, "user_id": user_id}, hint="client_doc_ref_1"
        )
        if document is None:
            raise HTTPException(404, "Document not found")
        return document

    def replay(self, existing: Record, submission: Submission) -> Record:
        if (
            existing["user_id"] != submission.user_id
            or existing["content_hash"] != content_hash(submission.content)
            or existing["title"] != submission.title
        ):
            raise HTTPException(409, "External reference is already assigned")
        return existing

    def submit(self, submission: Submission) -> tuple[Record, bool]:
        with self.storage.user_lock(submission.user_id):
            if submission.client_doc_ref:
                existing = self.documents.find_one(
                    {"client_doc_ref": submission.client_doc_ref}
                )
                if existing:
                    return self.replay(existing, submission), False
            cached = self.storage.cached(
                submission.user_id, content_hash(submission.content)
            )
            slot = None if cached else self.claim_slot(submission.user_id)
            document = {
                "_id": ObjectId(),
                "user_id": submission.user_id,
                "title": submission.title,
                "created_at": utcnow(),
                **pipeline_fields(submission.content, 1, cached),
            }
            if slot is not None:
                document["active_slot"] = slot
            if submission.client_doc_ref:
                document["client_doc_ref"] = submission.client_doc_ref
            try:
                self.documents.insert_one(document)
            except DuplicateKeyError:
                if submission.client_doc_ref:
                    existing = self.documents.find_one(
                        {"client_doc_ref": submission.client_doc_ref}
                    )
                    if existing:
                        return self.replay(existing, submission), False
                raise HTTPException(
                    429, "Active capacity changed; retry shortly"
                ) from None
            if slot is not None:
                self.storage.adjust_count(submission.user_id, 1)
            return document, True

    def update(self, document_id: str, user_id: str, update: ContentUpdate) -> Record:
        with self.storage.user_lock(user_id):
            old = self.get(document_id, user_id)
            # Omitting expected_version opts into last-writer-wins; the per-user
            # lock still serializes concurrent updates into separate versions.
            expected = (
                old["version"]
                if update.expected_version is None
                else update.expected_version
            )
            if old["version"] != expected:
                raise HTTPException(409, "Version changed; reload before updating")
            # PATCH always runs both stages, even when the content is cached.
            fields = pipeline_fields(update.content, expected + 1)
            slot = old.get("active_slot") if old["status"] in ACTIVE else None
            reactivating = slot is None
            if reactivating:
                slot = self.claim_slot(user_id)
            fields["active_slot"] = slot
            try:
                document = self.documents.find_one_and_update(
                    {"_id": old["_id"], "version": expected},
                    {"$set": fields},
                    return_document=ReturnDocument.AFTER,
                )
            except DuplicateKeyError:
                raise HTTPException(
                    429, "Active capacity changed; retry shortly"
                ) from None
            if document is None:
                raise HTTPException(409, "Version changed; reload before updating")
            if reactivating:
                self.storage.adjust_count(user_id, 1)
            return document

    def list(
        self, user_id: str, page: int, page_size: int, status: str | None
    ) -> dict[str, Any]:
        query: Record = {"user_id": user_id}
        if status:
            query["status"] = status
        documents = (
            self.documents.find(query)
            .sort([("created_at", -1), ("_id", -1)])
            .skip((page - 1) * page_size)
            .limit(page_size)
        )
        return {
            "items": [view(doc) for doc in documents],
            "page": page,
            "page_size": page_size,
            "total": self.documents.count_documents(query),
        }
