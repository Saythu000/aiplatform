import logging
from typing import List, Dict, Tuple, Any
from src.custom.search.schemas.search_schemas import SearchHit

logger = logging.getLogger(__name__)


class RRFFusion:
    """
    Reciprocal Rank Fusion (RRF) algorithm implementation.
    
    Combines multiple ranked lists into a single unified ranking
    using the formula: RRF_score = Σ(1 / (k + rank_i))
    """

    def __init__(self, k: int = 60):
        """
        Initialize RRF with k constant.
        
        Args:
            k: RRF constant, typically 60. Higher values reduce 
               the impact of top-ranked items.
        """
        self.k = k
        logger.info(f"RRFFusion initialized with k={k}")

    def fuse_results(
        self, 
        bm25_results: List[SearchHit], 
        semantic_results: List[SearchHit]
    ) -> List[Tuple[str, float, Dict[str, Any]]]:
        """
        Fuse BM25 and semantic search results using RRF.
        
        Args:
            bm25_results: Results from BM25/keyword search
            semantic_results: Results from vector/semantic search
            
        Returns:
            List of tuples: (document_id, rrf_score, document_data)
            Sorted by RRF score in descending order
        """
        rrf_scores = {}
        document_data = {}
        
        # Process BM25 results
        for rank, hit in enumerate(bm25_results, start=1):
            doc_id = hit.id
            rrf_score = 1.0 / (self.k + rank)
            
            rrf_scores[doc_id] = rrf_scores.get(doc_id, 0.0) + rrf_score
            document_data[doc_id] = {
                "source": hit.source,
                "highlight": hit.highlight,
                "bm25_rank": rank,
                "bm25_score": hit.score,
                "semantic_rank": None,
                "semantic_score": None
            }
            
        logger.debug(f"Processed {len(bm25_results)} BM25 results")
        
        # Process semantic results
        for rank, hit in enumerate(semantic_results, start=1):
            doc_id = hit.id
            rrf_score = 1.0 / (self.k + rank)
            
            rrf_scores[doc_id] = rrf_scores.get(doc_id, 0.0) + rrf_score
            
            if doc_id in document_data:
                # Document found in both searches
                document_data[doc_id]["semantic_rank"] = rank
                document_data[doc_id]["semantic_score"] = hit.score
            else:
                # Document only in semantic search
                document_data[doc_id] = {
                    "source": hit.source,
                    "highlight": hit.highlight,
                    "bm25_rank": None,
                    "bm25_score": None,
                    "semantic_rank": rank,
                    "semantic_score": hit.score
                }
                
        logger.debug(f"Processed {len(semantic_results)} semantic results")
        
        # Sort by RRF score (highest first)
        fused_results = [
            (doc_id, score, document_data[doc_id])
            for doc_id, score in sorted(
                rrf_scores.items(), 
                key=lambda x: x[1], 
                reverse=True
            )
        ]
        
        logger.info(
            f"RRF fusion complete: {len(fused_results)} unique documents, "
            f"top score: {fused_results[0][1]:.4f}"
        )
        
        return fused_results

    def explain_ranking(
        self, 
        doc_id: str, 
        bm25_results: List[SearchHit], 
        semantic_results: List[SearchHit]
    ) -> Dict[str, Any]:
        """
        Explain how a document's RRF score was calculated.
        
        Args:
            doc_id: Document ID to explain
            bm25_results: BM25 search results
            semantic_results: Semantic search results
            
        Returns:
            Dictionary with ranking explanation
        """
        explanation = {
            "document_id": doc_id,
            "k_value": self.k,
            "bm25_contribution": 0.0,
            "semantic_contribution": 0.0,
            "total_rrf_score": 0.0,
            "found_in": []
        }
        
        # Check BM25 results
        for rank, hit in enumerate(bm25_results, start=1):
            if hit.id == doc_id:
                contribution = 1.0 / (self.k + rank)
                explanation["bm25_contribution"] = contribution
                explanation["bm25_rank"] = rank
                explanation["bm25_score"] = hit.score
                explanation["found_in"].append("BM25")
                break
                
        # Check semantic results
        for rank, hit in enumerate(semantic_results, start=1):
            if hit.id == doc_id:
                contribution = 1.0 / (self.k + rank)
                explanation["semantic_contribution"] = contribution
                explanation["semantic_rank"] = rank
                explanation["semantic_score"] = hit.score
                explanation["found_in"].append("Semantic")
                break
                
        explanation["total_rrf_score"] = (
            explanation["bm25_contribution"] + 
            explanation["semantic_contribution"]
        )
        
        return explanation
