from typing import Dict
from .base import CredentialProvider
from src.custom.credentials.localsettings.opensearchconfig import OpenSearchConfig


class OpenSearchCredentials(CredentialProvider):
    """
    Provides OpenSearch credentials / configuration values
    in a clean & controlled way.
    """

    def __init__(self):
        self.config = OpenSearchConfig()

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
