from datetime import datetime
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, StringConstraints

UserId = Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_-]{1,64}$")]
Content = Annotated[
    str, StringConstraints(strip_whitespace=True, min_length=1, max_length=100000)
]
Status = Literal["queued", "processing", "enriching", "completed", "failed"]
StageStatus = Literal["pending", "running", "completed", "failed"]


class Submission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    user_id: UserId
    title: Annotated[
        str, StringConstraints(strip_whitespace=True, min_length=1, max_length=200)
    ]
    content: Content
    client_doc_ref: (
        Annotated[str, StringConstraints(pattern=r"^[a-zA-Z0-9_.:-]{1,128}$")] | None
    ) = None


class ContentUpdate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: Content
    expected_version: int | None = Field(default=None, ge=1)


class Stage(BaseModel):
    status: StageStatus = "pending"
    attempts: int = 0
    version: int | None = None
    error: str | None = None
    retry_at: datetime | None = None


class Summary(BaseModel):
    version: int
    content_hash: str
    text: str


class Tags(BaseModel):
    version: int
    content_hash: str
    values: list[str]


class DocumentView(BaseModel):
    document_id: str
    user_id: str
    title: str
    content: str
    client_doc_ref: str | None = None
    version: int
    content_hash: str
    status: Status
    processing: Stage
    enriching: Stage
    result_current: bool
    summary: Summary | None = None
    tags: Tags | None = None
    created_at: datetime
    updated_at: datetime


class DocumentPage(BaseModel):
    items: list[DocumentView]
    page: int
    page_size: int
    total: int
