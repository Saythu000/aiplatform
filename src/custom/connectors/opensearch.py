import logging
from typing import Dict, Any, Optional
from opensearchpy import OpenSearch

logger = logging.getLogger(__name__)

class OpenSearchConnector:
    """OpenSearch connector for platform integration."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self._client = None

    def __call__(self):
        """Return OpenSearch client connection."""
        if not self._client:
            self.connect()
        return self._client

    def connect(self) -> None:
        """Create OpenSearch client connection."""
        host = self.config.get("host", "localhost:9200")

        auth_config = {}
        if self.config.get("username") and self.config.get("password"):
            auth_config["http_auth"] = (self.config.get("username"), self.config.get("password"))

        self._client = OpenSearch(
            hosts=[host],
            use_ssl=self.config.get("use_ssl", False),
            verify_certs=self.config.get("verify_certs", False),
            ssl_show_warn=False,
            **auth_config
        )

        logger.info(f"OpenSearch client initialized with host: {host}")

    def health_check(self) -> bool:
        """Check if OpenSearch cluster is healthy."""
        try:
            health = self._client.cluster.health()
            return health["status"] in ["green", "yellow"]
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def create_index(self, index_name: str, mapping: Dict[str, Any]) -> bool:
        """Create index with proper mappings."""
        if self._client.indices.exists(index=index_name):
            logger.info(f"Index {index_name} already exists")
            return True

        try:
            self._client.indices.create(index=index_name, body=mapping)
            logger.info(f"Created index: {index_name}")
            return True
        except Exception as e:
            logger.error(f"Error creating index: {e}")
            return False

    def index_paper(self, index_name: str, paper_data: Dict[str, Any]) -> bool:
        """Index a single paper."""
        try:
            self._client.index(
                index=index_name,
                id=paper_data["arxiv_id"],
                body=paper_data
            )
            return True
        except Exception as e:
            logger.error(f"Error indexing paper: {e}")
            return False

    def search_papers(self, index_name: str, query: str, size: int = 10) -> Dict[str, Any]:
        """Search papers using BM25."""
        search_body = {
            "query": {
                "bool": {
                    "should": [
                        {
                            "multi_match": {
                                "query": query,
                                "fields": ["title^3", "abstract^2", "authors^1", "content^1"],
                                "type": "best_fields"
                            }
                        },
                        {
                            "term": {
                                "arxiv_id": query
                            }
                        }
                    ]
                }
            },
            "size": size,
            "highlight": {
                "fields": {
                    "title": {},
                    "abstract": {"fragment_size": 150}
                }
            }
        }

        try:
            response = self._client.search(index=index_name, body=search_body)
            return {
                "total": response["hits"]["total"]["value"],
                "hits": response["hits"]["hits"]
            }
        except Exception as e:
            logger.error(f"Search error: {e}")
            return {"total": 0, "hits": []}