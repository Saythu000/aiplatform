from opensearchpy import AsyncOpenSearch
from typing import Dict, Optional
import logging

logger = logging.getLogger(__name__)

class OpenSearchConnector:
    """
    Infrastructure Layer - OpenSearch Connector

    Follows the same pattern as ArxivConnector and JinaConnector
    """

    def __init__(self, config: Dict[str, str]):
        """
        Initialize OpenSearch connector with config.

        Expected config keys:
        - host: OpenSearch host URL
        - port: OpenSearch port
        - username: Auth username (optional)
        - password: Auth password (optional)
        - use_ssl: SSL usage (optional)
        """
        self.host = config["host"]
        self.port = config.get("port", 9200)
        self.username = config.get("username")
        self.password = config.get("password")
        self.use_ssl = config.get("use_ssl", False)

        self._client: Optional[AsyncOpenSearch] = None

        logger.info(
            "OpenSearchConnector initialized | host=%s port=%s",
            self.host, self.port
        )

    async def __call__(self) -> AsyncOpenSearch:
        """Callable version of connect()."""
        return await self.connect()

    async def connect(self) -> AsyncOpenSearch:
        """Create and return async OpenSearch client."""
        if self._client is None:
            # Build connection config
            auth = None
            if self.username and self.password:
                auth = (self.username, self.password)

            self._client = AsyncOpenSearch(
                hosts=[{"host": self.host, "port": self.port}],
                http_auth=auth,
                use_ssl=self.use_ssl,
                verify_certs=False,
                ssl_show_warn=False,
            )

            logger.info("Created new OpenSearch client session")

        return self._client

    async def close(self):
        """Close OpenSearch client connection."""
        if self._client:
            await self._client.close()
            self._client = None
            logger.info("OpenSearch client session closed")
