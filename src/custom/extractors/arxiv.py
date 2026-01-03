import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Optional
from urllib.parse import urlencode

class ArxivExtractor:
    def __init__(self, connection, config):
        self.connection = connection
        self.config = config
        self.base_url = "https://export.arxiv.org/api/query"
        self.namespaces = {
            'atom': 'http://www.w3.org/2005/Atom',
            'arxiv': 'http://arxiv.org/schemas/atom'
        }

    def __call__(self):
        return self.extract()

    def extract(self):
        """Extract papers from ArXiv API."""
        results = {}

        # Get configuration
        category = self.config.get("category", "cs.AI")
        max_results = self.config.get("max_results", 10)

        # Build query
        query_params = {
            'search_query': f'cat:{category}',
            'start': 0,
            'max_results': max_results,
            'sortBy': 'submittedDate',
            'sortOrder': 'descending'
        }

        url = f"{self.base_url}?{urlencode(query_params)}"

        try:
            response = self.connection.get(url, timeout=30)
            response.raise_for_status()

            papers = self._parse_response(response.text)
            results['arxiv_papers'] = papers

            print(f"Extracted {len(papers)} papers from ArXiv")
            return results

        except Exception as e:
            print(f"Error extracting from ArXiv: {e}")
            raise e

    def _parse_response(self, xml_content: str) -> List[dict]:
        """Parse ArXiv XML response to list of dictionaries."""
        papers = []

        try:
            root = ET.fromstring(xml_content)

            for entry in root.findall('atom:entry', self.namespaces):
                paper = {
                    'arxiv_id': self._extract_arxiv_id(entry),
                    'title': self._extract_text(entry, 'atom:title'),
                    'authors': self._extract_authors(entry),
                    'abstract': self._extract_text(entry, 'atom:summary'),
                    'published_date': self._extract_date(entry, 'atom:published'),
                    'pdf_url': self._extract_pdf_url(entry)
                }

                if paper['arxiv_id'] and paper['title']:
                    papers.append(paper)

        except ET.ParseError as e:
            print(f"Error parsing XML: {e}")

        return papers

    def _extract_arxiv_id(self, entry) -> Optional[str]:
        id_elem = entry.find('atom:id', self.namespaces)
        if id_elem is not None:
            return id_elem.text.split('/')[-1].split('v')[0]
        return None

    def _extract_text(self, entry, xpath: str) -> Optional[str]:
        elem = entry.find(xpath, self.namespaces)
        return elem.text.strip() if elem is not None else None

    def _extract_authors(self, entry) -> List[str]:
        authors = []
        for author in entry.findall('atom:author', self.namespaces):
            name_elem = author.find('atom:name', self.namespaces)
            if name_elem is not None:
                authors.append(name_elem.text)
        return authors

    def _extract_date(self, entry, xpath: str) -> Optional[str]:
        date_text = self._extract_text(entry, xpath)
        if date_text:
            try:
                dt = datetime.fromisoformat(date_text.replace('Z', '+00:00'))
                return dt.isoformat()
            except ValueError:
                pass
        return None

    def _extract_pdf_url(self, entry) -> Optional[str]:
        for link in entry.findall('atom:link', self.namespaces):
            if link.get('type') == 'application/pdf':
                return link.get('href')
        return None