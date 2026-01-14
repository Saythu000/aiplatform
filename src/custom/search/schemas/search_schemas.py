from typing import Dict, Any, List, Optional
from pydantic import BaseModel


class SearchRequest(BaseModel):
    """Search request model."""
    query: str
    size: int = 10
    from_: int = 0


class SearchHit(BaseModel):
    """Individual search result."""
    id: str
    score: float
    source: Dict[str, Any]
    highlight: Optional[Dict[str, List[str]]] = None


class SearchResponse(BaseModel):
    """Search response model."""
    total: int
    hits: List[SearchHit]
    took: Optional[int] = None


class IndexRequest(BaseModel):
    """Document indexing request."""
    arxiv_id: str
    title: str
    authors: str
    abstract: str
    content: str
    published_date: str
    pdf_url: str


class HealthStatus(BaseModel):
    """Health check response."""
    status: str
    cluster_name: Optional[str] = None
    number_of_nodes: Optional[int] = None
