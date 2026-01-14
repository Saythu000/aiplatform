"""
Hybrid Search Integration Example

This example demonstrates how to use the hybrid search system
with either Elasticsearch or OpenSearch backends.
"""

import asyncio
import logging
from typing import Dict, Any

from src.custom.credentials.factory import CredentialFactory
from src.custom.connectors.factory import ConnectorFactory
from src.custom.search.elasticsearch_service import ElasticsearchService
from src.custom.search.opensearch_service import OpenSearchService
from src.custom.search.hybrid_search_service import HybridSearchService
from src.custom.embeddings.jarxivembeddings import JinaEmbeddingsService
from src.custom.connectors.apiconnector import JinaConnector
from src.custom.search.schemas.hybrid_schemas import (
    HybridSearchRequest,
    HybridSearchConfig
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class HybridSearchExample:
    """Complete example of hybrid search setup and usage."""

    def __init__(self):
        self.search_service = None
        self.jina_service = None
        self.hybrid_service = None

    async def setup_elasticsearch(self) -> ElasticsearchService:
        """Setup Elasticsearch service."""
        logger.info("Setting up Elasticsearch service...")
        
        # Get credentials
        provider = CredentialFactory.get_provider(mode="elasticsearchlocal")
        config = provider.get_credentials()
        
        # Create connector
        connector = ConnectorFactory.get_connector("elasticsearch", config)
        
        # Create service
        service = ElasticsearchService(connector, index_name="papers")
        
        # Test connection
        health = await service.health_check()
        logger.info(f"Elasticsearch health: {health.status}")
        
        return service

    async def setup_opensearch(self) -> OpenSearchService:
        """Setup OpenSearch service."""
        logger.info("Setting up OpenSearch service...")
        
        # Get credentials
        provider = CredentialFactory.get_provider(mode="opensearchlocal")
        config = provider.get_credentials()
        
        # Create connector
        connector = ConnectorFactory.get_connector("opensearch", config)
        
        # Create service
        service = OpenSearchService(connector, index_name="papers")
        
        # Test connection
        health = await service.health_check()
        logger.info(f"OpenSearch health: {health.status}")
        
        return service

    async def setup_jina_embeddings(self) -> JinaEmbeddingsService:
        """Setup Jina embeddings service."""
        logger.info("Setting up Jina embeddings service...")
        
        # Jina configuration
        jina_config = {
            "base_url": "https://api.jina.ai/v1",
            "api_key": "your-jina-api-key",  # Set in .env as JINA_API_KEY
            "timeout_seconds": 30
        }
        
        # Embedding configuration
        embedding_config = {
            "model": "jina-embeddings-v3",
            "dimensions": 1024,
            "tasks": {
                "passage": "text-matching",
                "query": "text-matching"
            },
            "max_retries": 3,
            "base_backoff": 1.0
        }
        
        # Create connector
        jina_connector = JinaConnector(jina_config)
        
        # Create service
        service = JinaEmbeddingsService(jina_connector, embedding_config)
        
        return service

    async def setup_hybrid_search(self, search_engine: str = "elasticsearch"):
        """Setup complete hybrid search system."""
        logger.info(f"Setting up hybrid search with {search_engine}...")
        
        # Setup search service
        if search_engine == "elasticsearch":
            self.search_service = await self.setup_elasticsearch()
        elif search_engine == "opensearch":
            self.search_service = await self.setup_opensearch()
        else:
            raise ValueError("search_engine must be 'elasticsearch' or 'opensearch'")
        
        # Setup embeddings service
        self.jina_service = await self.setup_jina_embeddings()
        
        # Create hybrid search configuration
        config = HybridSearchConfig(
            default_k_value=60,
            default_size=10,
            default_search_engine=search_engine,
            parallel_search=True,
            timeout_seconds=30
        )
        
        # Create hybrid search service
        self.hybrid_service = HybridSearchService(
            search_engine=search_engine,
            search_service=self.search_service,
            embedding_service=self.jina_service,
            config=config
        )
        
        logger.info("Hybrid search setup complete!")

    async def example_search(self, query: str = "transformer neural networks"):
        """Perform example hybrid search."""
        if not self.hybrid_service:
            raise RuntimeError("Hybrid search not initialized. Call setup_hybrid_search() first.")
        
        logger.info(f"Performing hybrid search: '{query}'")
        
        # Create search request
        request = HybridSearchRequest(
            query=query,
            size=5,
            k_value=60,
            search_engine="elasticsearch",  # Will use the initialized engine
            include_explanation=True
        )
        
        # Perform search
        response = await self.hybrid_service.hybrid_search(request)
        
        # Display results
        print(f"\n🔍 Hybrid Search Results for: '{query}'")
        print(f"📊 Statistics:")
        print(f"   - Total unique documents: {response.total_unique}")
        print(f"   - BM25 results: {response.bm25_total}")
        print(f"   - Semantic results: {response.semantic_total}")
        print(f"   - Overlap: {response.overlap_count}")
        print(f"   - Search time: {response.took}ms")
        print(f"   - RRF k-value: {response.k_value}")
        
        print(f"\n📄 Top Results:")
        for i, result in enumerate(response.results, 1):
            print(f"{i}. Document: {result.id}")
            print(f"   RRF Score: {result.rrf_score:.4f}")
            print(f"   Found in: {', '.join(result.found_in)}")
            if result.bm25_rank:
                print(f"   BM25 Rank: {result.bm25_rank} (Score: {result.bm25_score:.3f})")
            if result.semantic_rank:
                print(f"   Semantic Rank: {result.semantic_rank} (Score: {result.semantic_score:.3f})")
            print(f"   Title: {result.source.get('title', 'N/A')[:100]}...")
            print()
        
        return response

    async def example_explanation(self, document_id: str, query: str):
        """Get ranking explanation for a specific document."""
        if not self.hybrid_service:
            raise RuntimeError("Hybrid search not initialized.")
        
        request = HybridSearchRequest(query=query, size=10)
        explanation = await self.hybrid_service.explain_ranking(document_id, request)
        
        print(f"\n🔍 Ranking Explanation for Document: {document_id}")
        print(f"Query: '{query}'")
        print(f"RRF k-value: {explanation.k_value}")
        print(f"Total RRF Score: {explanation.total_rrf_score:.4f}")
        print(f"Found in: {', '.join(explanation.found_in)}")
        
        if explanation.bm25_rank:
            print(f"BM25: Rank {explanation.bm25_rank}, Score {explanation.bm25_score:.3f}, Contribution {explanation.bm25_contribution:.4f}")
        
        if explanation.semantic_rank:
            print(f"Semantic: Rank {explanation.semantic_rank}, Score {explanation.semantic_score:.3f}, Contribution {explanation.semantic_contribution:.4f}")
        
        print(f"Explanation: {explanation.explanation_text}")

    async def cleanup(self):
        """Clean up all services."""
        logger.info("Cleaning up services...")
        
        if self.hybrid_service:
            await self.hybrid_service.close()
        
        if self.search_service:
            await self.search_service.close()
        
        # Note: Jina service cleanup should be handled by the connector
        logger.info("Cleanup complete!")


async def main():
    """Main example function."""
    example = HybridSearchExample()
    
    try:
        # Setup hybrid search (choose 'elasticsearch' or 'opensearch')
        await example.setup_hybrid_search(search_engine="elasticsearch")
        
        # Perform example searches
        await example.example_search("machine learning algorithms")
        await example.example_search("deep neural networks")
        await example.example_search("transformer architecture")
        
        # Example explanation (replace with actual document ID)
        # await example.example_explanation("2512.25052v1", "transformer architecture")
        
    except Exception as e:
        logger.error(f"Example failed: {e}")
        
    finally:
        # Cleanup
        await example.cleanup()


if __name__ == "__main__":
    # Run the example
    asyncio.run(main())
