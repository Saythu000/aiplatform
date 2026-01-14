from typing import Dict
from .base import CredentialProvider
from src.custom.credentials.localsettings.elasticsearchconfig import ElasticsearchConfig


class ElasticsearchCredentials(CredentialProvider):
    """
    Provides Elasticsearch credentials / configuration values
    in a clean & controlled way.
    """

    def __init__(self):
        self.config = ElasticsearchConfig()

    def get_credentials(self) -> Dict[str, str]:
        return {
            "host": self.config.host,
            "port": self.config.port,
            "username": self.config.username,
            "password": self.config.password,
            "use_ssl": self.config.use_ssl,
            "index_name": self.config.index_name,
            "timeout_seconds": self.config.timeout_seconds,
        }
