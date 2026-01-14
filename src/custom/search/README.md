# Hybrid Search System

A comprehensive hybrid search implementation that combines BM25 (keyword-based) and semantic (vector-based) search using Reciprocal Rank Fusion (RRF).

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    HybridSearchService                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────┐  │
│  │   BM25 Search   │    │ Semantic Search │    │   RRF   │  │
│  │   (Keywords)    │    │   (Vectors)     │    │ Fusion  │  │
│  └─────────────────┘    └─────────────────┘    └─────────┘  │
└─────────────────────────────────────────────────────────────┘
           │                        │                    │
           ▼                        ▼                    ▼
┌─────────────────┐    ┌─────────────────────────────────────┐
│ Elasticsearch/  │    │        JinaEmbeddingsService        │
│   OpenSearch    │    │                                     │
└─────────────────┘    └─────────────────────────────────────┘
```

## 🚀 Quick Start

### 1. Environment Setup

Add to your `.env` file:

```bash
# Elasticsearch
ELASTICSEARCH__HOST=localhost
ELASTICSEARCH__PORT=9200
ELASTICSEARCH__USERNAME=elastic
ELASTICSEARCH__PASSWORD=password

# OpenSearch  
OPENSEARCH__HOST=localhost
OPENSEARCH__PORT=9200
OPENSEARCH__USERNAME=admin
OPENSEARCH__PASSWORD=admin

# Jina Embeddings
JINA_API_KEY=your-jina-api-key
```

### 2. Basic Usage

```python
import asyncio
from src.custom.search.hybrid_search_example import HybridSearchExample

async def main():
    example = HybridSearchExample()
    
    # Setup with Elasticsearch
    await example.setup_hybrid_search(search_engine="elasticsearch")
    
    # Perform search
    await example.example_search("machine learning")
    
    # Cleanup
    await example.cleanup()

asyncio.run(main())
```

### 3. Advanced Usage

```python
from src.custom.search.hybrid_search_service import HybridSearchService
from src.custom.search.schemas.hybrid_schemas import HybridSearchRequest

# Initialize services (see example for full setup)
hybrid_service = HybridSearchService(
    search_engine="elasticsearch",
    search_service=elasticsearch_service,
    embedding_service=jina_service
)

# Create search request
request = HybridSearchRequest(
    query="transformer neural networks",
    size=10,
    k_value=60,
    include_explanation=True
)

# Perform hybrid search
response = await hybrid_service.hybrid_search(request)

# Process results
for result in response.results:
    print(f"Document: {result.id}")
    print(f"RRF Score: {result.rrf_score:.4f}")
    print(f"Found in: {', '.join(result.found_in)}")
```

## 📊 Components

### Core Services

| Component | Purpose | File |
|-----------|---------|------|
| `HybridSearchService` | Main orchestrator | `hybrid_search_service.py` |
| `RRFFusion` | Ranking fusion algorithm | `rrf_fusion.py` |
| `ElasticsearchService` | Elasticsearch operations | `elasticsearch_service.py` |
| `OpenSearchService` | OpenSearch operations | `opensearch_service.py` |
| `JinaEmbeddingsService` | Vector embeddings | `../embeddings/jarxivembeddings.py` |

### Schemas

| Schema | Purpose | Fields |
|--------|---------|--------|
| `HybridSearchRequest` | Search input | query, size, k_value, search_engine |
| `HybridSearchResponse` | Search output | results, statistics, timing |
| `RRFResult` | Individual result | id, rrf_score, rankings, metadata |
| `SearchExplanation` | Ranking details | contributions, explanations |

## 🔧 Configuration

### HybridSearchConfig

```python
config = HybridSearchConfig(
    default_k_value=60,           # RRF constant
    default_size=10,              # Results per search
    default_search_engine="elasticsearch",
    parallel_search=True,         # Run searches in parallel
    timeout_seconds=30,           # Search timeout
    enable_explanation=True,      # Allow ranking explanations
    enable_highlighting=True      # Enable search highlighting
)
```

### RRF Parameters

- **k_value**: RRF constant (default: 60)
  - Lower values: More weight to top-ranked items
  - Higher values: More balanced ranking
- **size**: Number of results to return
- **parallel_search**: Run BM25 and semantic searches simultaneously

## 🔍 Search Types

### 1. BM25 Search (Keyword-based)
- **Purpose**: Traditional text matching
- **Good for**: Exact terms, names, specific concepts
- **Fields**: title^3, abstract^2, authors^1, content^1

### 2. Semantic Search (Vector-based)
- **Purpose**: Meaning-based similarity
- **Good for**: Conceptual queries, synonyms, related topics
- **Model**: Jina Embeddings v3 (1024 dimensions)

### 3. Hybrid Search (RRF Fusion)
- **Purpose**: Best of both worlds
- **Algorithm**: Reciprocal Rank Fusion
- **Formula**: `RRF_score = Σ(1 / (k + rank_i))`

## 📈 Performance

### Search Statistics

Each search returns detailed statistics:

```python
response = await hybrid_service.hybrid_search(request)

print(f"Total unique documents: {response.total_unique}")
print(f"BM25 results: {response.bm25_total}")
print(f"Semantic results: {response.semantic_total}")
print(f"Overlap: {response.overlap_count}")
print(f"Search time: {response.took}ms")
```

### Optimization Tips

1. **Parallel Search**: Enable for better performance
2. **Result Size**: Use `size * 2` for fusion to get better ranking
3. **Index Optimization**: Ensure proper mappings for vector fields
4. **Caching**: Consider caching embeddings for repeated queries

## 🛠️ Troubleshooting

### Common Issues

#### 1. Connection Errors
```bash
# Check service health
curl http://localhost:9200/_cluster/health  # Elasticsearch
curl http://localhost:9200/_cluster/health  # OpenSearch
```

#### 2. Embedding Errors
- Verify Jina API key in environment
- Check network connectivity to Jina API
- Monitor rate limits

#### 3. No Results
- Ensure index exists and has data
- Check query syntax
- Verify field mappings include `embedding` field

### Debug Mode

Enable detailed logging:

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## 🔄 Airflow Integration

### Separate DAGs (As Requested)

You mentioned creating separate DAGs for:

1. **OpenSearch DAG**: Index management and data ingestion
2. **Elasticsearch DAG**: Index management and data ingestion  
3. **RRF DAG**: Hybrid search operations and monitoring

Example DAG structure:
```python
# dags/opensearch_indexing_dag.py
# dags/elasticsearch_indexing_dag.py
# dags/hybrid_search_dag.py
```

## 📚 API Reference

### HybridSearchService Methods

```python
# Main search method
async def hybrid_search(request: HybridSearchRequest) -> HybridSearchResponse

# Get ranking explanation
async def explain_ranking(document_id: str, request: HybridSearchRequest) -> SearchExplanation

# Cleanup
async def close()
```

### RRFFusion Methods

```python
# Fuse search results
def fuse_results(bm25_results: List[SearchHit], semantic_results: List[SearchHit]) -> List[Tuple]

# Explain ranking for specific document
def explain_ranking(doc_id: str, bm25_results: List, semantic_results: List) -> Dict
```

## 🧪 Testing

Run the integration example:

```bash
cd /home/surya/aiplatform
python -m src.custom.search.hybrid_search_example
```

## 📝 Next Steps

1. **Index Your Data**: Use existing TextChunker to prepare documents
2. **Create Airflow DAGs**: Separate DAGs for each search engine
3. **Monitor Performance**: Track search latency and relevance
4. **Tune Parameters**: Adjust k_value and weights based on results
5. **Add Filters**: Implement metadata filtering (date, author, category)

## 🤝 Contributing

When extending the hybrid search system:

1. Follow existing patterns in `*_service.py` files
2. Add new schemas to `schemas/hybrid_schemas.py`
3. Update this README with new features
4. Add logging for debugging
5. Include error handling for robustness

---

**Happy Searching! 🔍✨**
