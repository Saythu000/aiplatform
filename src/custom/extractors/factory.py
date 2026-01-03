from .rdbms import RDBMSExtractor
from .arxiv import ArxivExtractor

class ExtractorFactory:
    @staticmethod
    def get_extractor(extractor_type: str, connection: str, config: dict):
        if extractor_type == "rdbms":
            return RDBMSExtractor(connection=connection, config=config)
        elif extractor_type == "gmail":
            return GmailExtractor(connection=connection, config=config)
        elif extractor_type == "arxiv":
            return ArxivExtractor(connection=connection, config=config)
        else:
            raise ValueError(f"Unknown extractor type: {extractor_type}")