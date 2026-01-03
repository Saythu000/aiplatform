import requests
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import List, Optional
from urllib.parse import urlencode

class ArxivConnector:
    def __init__(self, config):
        self.config = config
        self.base_url = "https://export.arxiv.org/api/query"
        self.namespaces = {
            'atom': 'http://www.w3.org/2005/Atom',
            'arxiv': 'http://arxiv.org/schemas/atom'
        }
        self._session = None

    def __call__(self):
        self.connect()
        return self._session

    def connect(self):
        """Create HTTP session for ArXiv API."""
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'aiplatform-arxiv-connector/1.0'
        })