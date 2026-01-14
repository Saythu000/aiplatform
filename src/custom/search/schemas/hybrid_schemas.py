from typing import Dict, Any, List, Optional
from pydantic import BaseModel, Field


class HybridSearchRequest(BaseModel):
    """Hybrid search request model."""
    query: str = Field(..., description="Search query text")
    size: int = Field(default=10, ge=1, le=100, description="Number of results to return")
    k_value: int = Field(default=60, ge=1, description="RRF k constant")
    search_engine: str = Field(default="elasticsearch", description="Search engine to use")
    include_explanation: bool = Field(default=False, description="Include ranking explanation")
    bm25_weight: float = Field(default=1.0, ge=0.0, description="Weight for BM25 results")
    semantic_weight: float = Field(default=1.0, ge=0.0, description="Weight for semantic results")


class RRFResult(BaseModel):
    """Individual RRF search result."""
    id: str = Field(..., description="Document ID")
    rrf_score: float = Field(..., description="Combined RRF score")
    source: Dict[str, Any] = Field(..., description="Document source data")
    highlight: Optional[Dict[str, List[str]]] = Field(None, description="Search highlights")
    
    # Ranking details
    bm25_rank: Optional[int] = Field(None, description="Rank in BM25 results")
    bm25_score: Optional[float] = Field(None, description="BM25 relevance score")
    semantic_rank: Optional[int] = Field(None, description="Rank in semantic results")
    semantic_score: Optional[float] = Field(None, description="Semantic similarity score")
    
    # RRF breakdown
    bm25_contribution: float = Field(default=0.0, description="BM25 contribution to RRF score")
    semantic_contribution: float = Field(default=0.0, description="Semantic contribution to RRF score")
    found_in: List[str] = Field(default_factory=list, description="Which searches found this document")


class HybridSearchResponse(BaseModel):
    """Hybrid search response model."""
    total_unique: int = Field(..., description="Total unique documents found")
    results: List[RRFResult] = Field(..., description="RRF-ranked results")
    took: Optional[int] = Field(None, description="Search time in milliseconds")
    
    # Search statistics
    bm25_total: int = Field(default=0, description="Total BM25 results")
    semantic_total: int = Field(default=0, description="Total semantic results")
    overlap_count: int = Field(default=0, description="Documents found in both searches")
    
    # Configuration used
    k_value: int = Field(..., description="RRF k constant used")
    search_engine: str = Field(..., description="Search engine used")


class SearchExplanation(BaseModel):
    """Detailed explanation of how a document was ranked."""
    document_id: str = Field(..., description="Document ID")
    k_value: int = Field(..., description="RRF k constant used")
    
    # BM25 details
    bm25_rank: Optional[int] = Field(None, description="Rank in BM25 results")
    bm25_score: Optional[float] = Field(None, description="BM25 relevance score")
    bm25_contribution: float = Field(default=0.0, description="BM25 RRF contribution")
    
    # Semantic details
    semantic_rank: Optional[int] = Field(None, description="Rank in semantic results")
    semantic_score: Optional[float] = Field(None, description="Semantic similarity score")
    semantic_contribution: float = Field(default=0.0, description="Semantic RRF contribution")
    
    # Final result
    total_rrf_score: float = Field(..., description="Combined RRF score")
    found_in: List[str] = Field(..., description="Which searches found this document")
    
    # Human-readable explanation
    explanation_text: str = Field(default="", description="Human-readable ranking explanation")


class HybridSearchConfig(BaseModel):
    """Configuration for hybrid search service."""
    default_k_value: int = Field(default=60, description="Default RRF k constant")
    default_size: int = Field(default=10, description="Default result size")
    default_search_engine: str = Field(default="elasticsearch", description="Default search engine")
    max_results: int = Field(default=100, description="Maximum results allowed")
    
    # Performance settings
    parallel_search: bool = Field(default=True, description="Run searches in parallel")
    timeout_seconds: int = Field(default=30, description="Search timeout")
    
    # Feature flags
    enable_explanation: bool = Field(default=True, description="Allow ranking explanations")
    enable_highlighting: bool = Field(default=True, description="Enable search highlighting")
