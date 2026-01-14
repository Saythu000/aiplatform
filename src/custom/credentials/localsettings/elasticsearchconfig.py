from pathlib import Path
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class ElasticsearchConfig(BaseSettings):
    """
    Elasticsearch configuration.
    Loads from environment variables with ELASTICSEARCH__ prefix.
    """

    model_config = SettingsConfigDict(
        env_prefix="ELASTICSEARCH__",
        env_file=[".env"],
        extra="ignore",
        case_sensitive=False,
    )

    # Core connection settings
    host: str = "localhost"
    port: int = 9200
    username: str = None
    password: str = None
    use_ssl: bool = False
    
    # Index settings
    index_name: str = "papers"
    timeout_seconds: int = 30
