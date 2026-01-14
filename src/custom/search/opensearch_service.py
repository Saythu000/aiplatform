import logging
from typing import Dict, Any, Optional, List
from opensearchpy import AsyncOpenSearch

from src.custom.connectors.opensearch import OpenSearchConnector
from src.custom.search.schemas.search_schemas import (
    SearchRequest,
    SearchResponse,
    SearchHit,
    IndexRequest,
    HealthStatus
)

logger = logging.getLogger(__name__)


class OpenSearchService:
    """
    Service Layer - OpenSearch Operations
    
    Handles all OpenSearch business logic:
    - Document indexing
    - BM25 search
    - Index management
    - Health monitoring
    """

    def __init__(self, connector: OpenSearchConnector, index_name: str = "papers"):
        """
        Initialize OpenSearch service.
        
        Args:
            connector: OpenSearchConnector instance
            index_name: Name of the index to use
        """
        self.connector = connector
        self.index_name = index_name
        
        logger.info(f"OpenSearchService initialized with index: {index_name}")

    async def health_check(self) -> HealthStatus:
        """Check if OpenSearch cluster is healthy."""
        try:
            client = await self.connector.connect()
            health = await client.cluster.health()
            
            return HealthStatus(
                status=health["status"],
                cluster_name=health.get("cluster_name"),
                number_of_nodes=health.get("number_of_nodes")
            )
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthStatus(status="red")

    async def create_index(self) -> bool:
        """Create papers index with proper mappings."""
        try:
            client = await self.connector.connect()
            
            # Check if index exists
            exists = await client.indices.exists(index=self.index_name)
            if exists:
                logger.info(f"Index {self.index_name} already exists")
                return True

            # Create index with mapping
            mapping = {
                "mappings": {
                    "properties": {
                        "arxiv_id": {"type": "keyword"},
                        "title": {"type": "text", "analyzer": "english"},
                        "authors": {"type": "text", "analyzer": "english"},
                        "abstract": {"type": "text", "analyzer": "english"},
                        "content": {"type": "text", "analyzer": "english"},
                        "published_date": {"type": "date"},
                        "pdf_url": {"type": "keyword"},
                        "embedding": {
                            "type": "dense_vector",
                            "dims": 1024,
                            "index": True,
                            "similarity": "cosine"
                        }
                    }
                }
            }

            await client.indices.create(index=self.index_name, body=mapping)
            logger.info(f"Created index: {self.index_name}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            return False

    async def index_paper(self, paper_data: IndexRequest) -> bool:
        """Index a single paper."""
        try:
            client = await self.connector.connect()
            
            await client.index(
                index=self.index_name,
                id=paper_data.arxiv_id,
                body=paper_data.model_dump()
            )
            
            logger.info(f"Indexed paper: {paper_data.arxiv_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error indexing paper {paper_data.arxiv_id}: {e}")
            return False

    async def index_papers_bulk(self, papers: List[IndexRequest]) -> Dict[str, int]:
        """Bulk index multiple papers."""
        try:
            client = await self.connector.connect()
            
            # Prepare bulk operations
            operations = []
            for paper in papers:
                operations.append({
                    "index": {
                        "_index": self.index_name,
                        "_id": paper.arxiv_id
                    }
                })
                operations.append(paper.model_dump())

            # Execute bulk operation
            response = await client.bulk(body=operations)
            
            # Count results
            indexed = 0
            errors = 0
            
            for item in response["items"]:
                if "index" in item:
                    if item["index"].get("status") in [200, 201]:
                        indexed += 1
                    else:
                        errors += 1

            logger.info(f"Bulk indexed: {indexed} papers, {errors} errors")
            return {"indexed": indexed, "errors": errors}
            
        except Exception as e:
            logger.error(f"Bulk indexing failed: {e}")
            return {"indexed": 0, "errors": len(papers)}

    async def search_papers(self, request: SearchRequest) -> SearchResponse:
        """Search papers using BM25."""
        try:
            client = await self.connector.connect()
            
            search_body = {
                "query": {
                    "bool": {
                        "should": [
                            {
                                "multi_match": {
                                    "query": request.query,
                                    "fields": ["title^3", "abstract^2", "authors^1", "content^1"],
                                    "type": "best_fields"
                                }
                            },
                            {
                                "term": {
                                    "arxiv_id": request.query
                                }
                            }
                        ]
                    }
                },
                "size": request.size,
                "from": request.from_,
                "highlight": {
                    "fields": {
                        "title": {},
                        "abstract": {"fragment_size": 150}
                    }
                }
            }

            response = await client.search(index=self.index_name, body=search_body)
            
            # Convert to SearchResponse
            hits = []
            for hit in response["hits"]["hits"]:
                hits.append(SearchHit(
                    id=hit["_id"],
                    score=hit["_score"],
                    source=hit["_source"],
                    highlight=hit.get("highlight")
                ))

            return SearchResponse(
                total=response["hits"]["total"]["value"],
                hits=hits,
                took=response.get("took")
            )
            
        except Exception as e:
            logger.error(f"Search error: {e}")
            return SearchResponse(total=0, hits=[])

    async def vector_search(self, query_vector: List[float], size: int = 10) -> SearchResponse:
        """Perform semantic search using vector similarity."""
        try:
            client = await self.connector.connect()
            
            search_body = {
                "query": {
                    "knn": {
                        "embedding": {
                            "vector": query_vector,
                            "k": size
                        }
                    }
                },
                "size": size
            }

            response = await client.search(index=self.index_name, body=search_body)
            
            # Convert to SearchResponse
            hits = []
            for hit in response["hits"]["hits"]:
                hits.append(SearchHit(
                    id=hit["_id"],
                    score=hit["_score"],
                    source=hit["_source"]
                ))

            return SearchResponse(
                total=response["hits"]["total"]["value"],
                hits=hits,
                took=response.get("took")
            )
            
        except Exception as e:
            logger.error(f"Vector search error: {e}")
            return SearchResponse(total=0, hits=[])

    async def delete_index(self) -> bool:
        """Delete the papers index."""
        try:
            client = await self.connector.connect()
            await client.indices.delete(index=self.index_name)
            logger.info(f"Deleted index: {self.index_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting index: {e}")
            return False

    async def close(self):
        """Close the connector."""
        await self.connector.close()
