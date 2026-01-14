from elasticsearch import AsyncElasticsearch
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class ElasticsearchConnector:
    """
    Infrastructure Layer - Elasticsearch Connector

    Follows the same pattern as OpenSearchConnector
    """

    def __init__(self, config: Dict[str, str]):
        """
        Initialize Elasticsearch connector with config.

        Expected config keys:
        - host: Elasticsearch host URL
        - port: Elasticsearch port
        - username: Auth username (optional)
        - password: Auth password (optional)
        - use_ssl: SSL usage (optional)
        """
        self.host = config["host"]
        self.port = config.get("port", 9200)
        self.username = config.get("username")
        self.password = config.get("password")
        self.use_ssl = config.get("use_ssl", False)

        self._client: Optional[AsyncElasticsearch] = None

        logger.info(
            "ElasticsearchConnector initialized | host=%s port=%s",
            self.host, self.port
        )

    async def __call__(self) -> AsyncElasticsearch:
        """Callable version of connect()."""
        return await self.connect()

    async def connect(self) -> AsyncElasticsearch:
        """Create and return async Elasticsearch client."""
        if self._client is None:
            # Build connection config
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)

            self._client = AsyncElasticsearch(
                hosts=[{"host": self.host, "port": self.port}],
                http_auth=auth,
                use_ssl=self.use_ssl,
                verify_certs=False,
                ssl_show_warn=False,
            )

            logger.info("Created new Elasticsearch client session")

        return self._client

    async def close(self):
        """Close Elasticsearch client connection."""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("Elasticsearch client session closed")
