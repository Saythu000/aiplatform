import asyncio
import logging
import time
from typing import Union, List

from src.custom.search.elasticsearch_service import ElasticsearchService
from src.custom.search.opensearch_service import OpenSearchService
from src.custom.search.rrf_fusion import RRFFusion
from src.custom.embeddings.jarxivembeddings import JinaEmbeddingsService
from src.custom.search.schemas.hybrid_schemas import (
    HybridSearchRequest,
    HybridSearchResponse,
    RRFResult,
    SearchExplanation,
    HybridSearchConfig
)
from src.custom.search.schemas.search_schemas import SearchRequest

logger = logging.getLogger(__name__)


class HybridSearchService:
    """
    Hybrid Search Service - Combines BM25 and Semantic Search using RRF
    
    Supports either Elasticsearch or OpenSearch as the search backend.
    Uses JinaEmbeddingsService for semantic search capabilities.
    """

    def __init__(
        self,
        search_engine: str,
        search_service: Union[ElasticsearchService, OpenSearchService],
        embedding_service: JinaEmbeddingsService,
        config: HybridSearchConfig = None
    ):
        """
        Initialize hybrid search service.
        
        Args:
            search_engine: "elasticsearch" or "opensearch"
            search_service: Initialized search service instance
            embedding_service: Initialized Jina embeddings service
            config: Optional configuration settings
        """
        if search_engine not in ["elasticsearch", "opensearch"]:
            raise ValueError("search_engine must be 'elasticsearch' or 'opensearch'")
            
        self.search_engine = search_engine
        self.search_service = search_service
        self.embedding_service = embedding_service
        self.config = config or HybridSearchConfig()
        
        # Initialize RRF with default k-value
        self.rrf = RRFFusion(k=self.config.default_k_value)
        
        logger.info(
            f"HybridSearchService initialized | engine={search_engine} "
            f"k={self.config.default_k_value}"
        )

    async def hybrid_search(self, request: HybridSearchRequest) -> HybridSearchResponse:
        """
        Perform hybrid search combining BM25 and semantic search.
        
        Args:
            request: Hybrid search request
            
        Returns:
            HybridSearchResponse with RRF-ranked results
        """
        start_time = time.time()
        
        # Update RRF k-value if different from default
        if request.k_value != self.rrf.k:
            self.rrf = RRFFusion(k=request.k_value)
        
        logger.info(f"Starting hybrid search: '{request.query}' | size={request.size}")
        
        try:
            # Run both searches in parallel if enabled
            if self.config.parallel_search:
                bm25_results, semantic_results = await asyncio.gather(
                    self._bm25_search(request),
                    self._semantic_search(request),
                    return_exceptions=True
                )
                
                # Handle exceptions
                if isinstance(bm25_results, Exception):
                    logger.error(f"BM25 search failed: {bm25_results}")
                    bm25_results = []
                    
                if isinstance(semantic_results, Exception):
                    logger.error(f"Semantic search failed: {semantic_results}")
                    semantic_results = []
                    
            else:
                # Run searches sequentially
                bm25_results = await self._bm25_search(request)
                semantic_results = await self._semantic_search(request)
            
            # Apply RRF fusion
            fused_results = self.rrf.fuse_results(bm25_results, semantic_results)
            
            # Convert to RRFResult objects
            rrf_results = []
            for doc_id, rrf_score, doc_data in fused_results[:request.size]:
                rrf_result = RRFResult(
                    id=doc_id,
                    rrf_score=rrf_score,
                    source=doc_data["source"],
                    highlight=doc_data["highlight"],
                    bm25_rank=doc_data["bm25_rank"],
                    bm25_score=doc_data["bm25_score"],
                    semantic_rank=doc_data["semantic_rank"],
                    semantic_score=doc_data["semantic_score"],
                    bm25_contribution=rrf_score - (1.0 / (request.k_value + (doc_data["semantic_rank"] or 999)) if doc_data["semantic_rank"] else 0),
                    semantic_contribution=rrf_score - (1.0 / (request.k_value + (doc_data["bm25_rank"] or 999)) if doc_data["bm25_rank"] else 0),
                    found_in=self._get_found_in(doc_data)
                )
                rrf_results.append(rrf_result)
            
            # Calculate statistics
            overlap_count = sum(1 for _, _, doc_data in fused_results 
                              if doc_data["bm25_rank"] and doc_data["semantic_rank"])
            
            took = int((time.time() - start_time) * 1000)
            
            response = HybridSearchResponse(
                total_unique=len(fused_results),
                results=rrf_results,
                took=took,
                bm25_total=len(bm25_results),
                semantic_total=len(semantic_results),
                overlap_count=overlap_count,
                k_value=request.k_value,
                search_engine=request.search_engine
            )
            
            logger.info(
                f"Hybrid search complete | unique={len(fused_results)} "
                f"overlap={overlap_count} took={took}ms"
            )
            
            return response
            
        except Exception as e:
            logger.error(f"Hybrid search failed: {e}")
            return HybridSearchResponse(
                total_unique=0,
                results=[],
                took=int((time.time() - start_time) * 1000),
                bm25_total=0,
                semantic_total=0,
                overlap_count=0,
                k_value=request.k_value,
                search_engine=request.search_engine
            )

    async def _bm25_search(self, request: HybridSearchRequest):
        """Perform BM25/keyword search."""
        search_request = SearchRequest(
            query=request.query,
            size=request.size * 2  # Get more results for better fusion
        )
        
        response = await self.search_service.search_papers(search_request)
        logger.debug(f"BM25 search returned {len(response.hits)} results")
        return response.hits

    async def _semantic_search(self, request: HybridSearchRequest):
        """Perform semantic/vector search."""
        # Generate query embedding
        query_vector = await self.embedding_service.embed_query(request.query)
        
        # Perform vector search
        response = await self.search_service.vector_search(
            query_vector=query_vector,
            size=request.size * 2  # Get more results for better fusion
        )
        
        logger.debug(f"Semantic search returned {len(response.hits)} results")
        return response.hits

    def _get_found_in(self, doc_data: dict) -> List[str]:
        """Determine which searches found the document."""
        found_in = []
        if doc_data["bm25_rank"]:
            found_in.append("BM25")
        if doc_data["semantic_rank"]:
            found_in.append("Semantic")
        return found_in

    async def explain_ranking(
        self, 
        document_id: str, 
        request: HybridSearchRequest
    ) -> SearchExplanation:
        """
        Explain how a document was ranked in hybrid search.
        
        Args:
            document_id: Document ID to explain
            request: Original search request
            
        Returns:
            SearchExplanation with ranking details
        """
        # Run searches to get ranking positions
        bm25_results = await self._bm25_search(request)
        semantic_results = await self._semantic_search(request)
        
        # Get explanation from RRF
        explanation_data = self.rrf.explain_ranking(
            document_id, bm25_results, semantic_results
        )
        
        # Create human-readable explanation
        explanation_text = self._create_explanation_text(explanation_data)
        
        return SearchExplanation(
            document_id=document_id,
            k_value=explanation_data["k_value"],
            bm25_rank=explanation_data.get("bm25_rank"),
            bm25_score=explanation_data.get("bm25_score"),
            bm25_contribution=explanation_data["bm25_contribution"],
            semantic_rank=explanation_data.get("semantic_rank"),
            semantic_score=explanation_data.get("semantic_score"),
            semantic_contribution=explanation_data["semantic_contribution"],
            total_rrf_score=explanation_data["total_rrf_score"],
            found_in=explanation_data["found_in"],
            explanation_text=explanation_text
        )

    def _create_explanation_text(self, explanation_data: dict) -> str:
        """Create human-readable explanation text."""
        found_in = explanation_data["found_in"]
        total_score = explanation_data["total_rrf_score"]
        
        if len(found_in) == 2:  # Found in both
            return (
                f"Document found in both BM25 (rank {explanation_data.get('bm25_rank')}) "
                f"and semantic search (rank {explanation_data.get('semantic_rank')}). "
                f"RRF score: {total_score:.4f}"
            )
        elif "BM25" in found_in:
            return (
                f"Document found only in BM25 search at rank {explanation_data.get('bm25_rank')}. "
                f"RRF score: {total_score:.4f}"
            )
        elif "Semantic" in found_in:
            return (
                f"Document found only in semantic search at rank {explanation_data.get('semantic_rank')}. "
                f"RRF score: {total_score:.4f}"
            )
        else:
            return f"Document not found in search results. RRF score: {total_score:.4f}"

    async def close(self):
        """Close all service connections."""
        await self.search_service.close()
        # Note: embedding_service.close() should be called by the caller
        logger.info("HybridSearchService closed")
