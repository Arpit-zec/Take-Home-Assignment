from typing import Annotated

from fastapi import APIRouter, Depends, Header, HTTPException, Query, Request, Response

from app.models import (
    ContentUpdate,
    DocumentPage,
    DocumentView,
    Status,
    Submission,
    UserId,
)
from app.service import DocumentService, view

router = APIRouter()


def current_user(x_user_id: Annotated[UserId, Header()]) -> str:
    # POC identity; a production gateway must replace this with verified claims.
    return x_user_id


def service(request: Request) -> DocumentService:
    return DocumentService(request.app.state.storage)


User = Annotated[str, Depends(current_user)]
Service = Annotated[DocumentService, Depends(service)]


@router.post("/documents", response_model=DocumentView, status_code=201)
def submit(
    body: Submission, response: Response, user: User, documents: Service
) -> DocumentView:
    if body.user_id != user:
        raise HTTPException(403, "user_id must match the authenticated user")
    document, created = documents.submit(body)
    response.status_code = 201 if created else 200
    return view(document)


# Register before the dynamic id route.
@router.get("/documents/by-ref/{client_doc_ref}", response_model=DocumentView)
def lookup(client_doc_ref: str, user: User, documents: Service) -> DocumentView:
    return view(documents.by_ref(client_doc_ref, user))


@router.get("/documents/{document_id}", response_model=DocumentView)
def get(document_id: str, user: User, documents: Service) -> DocumentView:
    return view(documents.get(document_id, user))


@router.patch("/documents/{document_id}", response_model=DocumentView)
def update(
    document_id: str, body: ContentUpdate, user: User, documents: Service
) -> DocumentView:
    return view(documents.update(document_id, user, body))


@router.get("/users/{user_id}/documents", response_model=DocumentPage)
def listing(
    user_id: str,
    user: User,
    documents: Service,
    page: Annotated[int, Query(ge=1, le=100000)] = 1,
    page_size: Annotated[int, Query(ge=1, le=100)] = 20,
    status: Status | None = None,
) -> DocumentPage:
    if user_id != user:
        raise HTTPException(404, "User not found")
    return DocumentPage.model_validate(documents.list(user, page, page_size, status))
