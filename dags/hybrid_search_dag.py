from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import asyncio
import logging

from src.custom.credentials.factory import CredentialFactory
from src.custom.connectors.factory import ConnectorFactory
from src.custom.search.elasticsearch_service import ElasticsearchService
from src.custom.search.opensearch_service import OpenSearchService
from src.custom.search.hybrid_search_service import HybridSearchService
from src.custom.embeddings.jarxivembeddings import JinaEmbeddingsService
from src.custom.connectors.apiconnector import JinaConnector
from src.custom.search.schemas.hybrid_schemas import HybridSearchRequest, HybridSearchConfig

logger = logging.getLogger(__name__)

def setup_hybrid_search_services(**kwargs):
    """
    Task 1: Setup and validate all services for hybrid search.
    """
    print("Setting up hybrid search services...")
    
    # Get search engine from DAG config (default to elasticsearch)
    search_engine = kwargs.get('dag_run').conf.get('search_engine', 'elasticsearch') if kwargs.get('dag_run') and kwargs.get('dag_run').conf else 'elasticsearch'
    
    async def setup_services():
        services = {}
        
        # Setup search service based on engine choice
        if search_engine == "elasticsearch":
            provider = CredentialFactory.get_provider(mode="elasticsearchlocal")
            config = provider.get_credentials()
            connector = ConnectorFactory.get_connector("elasticsearch", config)
            services['search_service'] = ElasticsearchService(connector, index_name="papers")
        else:
            provider = CredentialFactory.get_provider(mode="opensearchlocal")
            config = provider.get_credentials()
            connector = ConnectorFactory.get_connector("opensearch", config)
            services['search_service'] = OpenSearchService(connector, index_name="papers")
        
        # Setup Jina embeddings service
        jina_config = {
            "base_url": "https://api.jina.ai/v1",
            "api_key": "your-jina-api-key",  # Should be in environment
            "timeout_seconds": 30
        }
        
        embedding_config = {
            "model": "jina-embeddings-v3",
            "dimensions": 1024,
            "tasks": {
                "passage": "text-matching",
                "query": "text-matching"
            },
            "max_retries": 3,
            "base_backoff": 1.0
        }
        
        jina_connector = JinaConnector(jina_config)
        services['jina_service'] = JinaEmbeddingsService(jina_connector, embedding_config)
        
        # Test services
        health = await services['search_service'].health_check()
        print(f"{search_engine.title()} health: {health.status}")
        
        # Close services for now (will be recreated in other tasks)
        await services['search_service'].close()
        
        return {
            'search_engine': search_engine,
            'search_health': health.status,
            'services_ready': True
        }
    
    result = asyncio.run(setup_services())
    return result

def perform_hybrid_search_test(ti, **kwargs):
    """
    Task 2: Perform test hybrid searches with different queries.
    """
    setup_info = ti.xcom_pull(task_ids='setup_services_task')
    search_engine = setup_info['search_engine']
    
    print(f"Performing hybrid search tests with {search_engine}...")
    
    async def run_hybrid_searches():
        # Setup services
        if search_engine == "elasticsearch":
            provider = CredentialFactory.get_provider(mode="elasticsearchlocal")
            config = provider.get_credentials()
            connector = ConnectorFactory.get_connector("elasticsearch", config)
            search_service = ElasticsearchService(connector, index_name="papers")
        else:
            provider = CredentialFactory.get_provider(mode="opensearchlocal")
            config = provider.get_credentials()
            connector = ConnectorFactory.get_connector("opensearch", config)
            search_service = OpenSearchService(connector, index_name="papers")
        
        # Setup Jina service
        jina_config = {
            "base_url": "https://api.jina.ai/v1",
            "api_key": "your-jina-api-key",
            "timeout_seconds": 30
        }
        
        embedding_config = {
            "model": "jina-embeddings-v3",
            "dimensions": 1024,
            "tasks": {"passage": "text-matching", "query": "text-matching"},
            "max_retries": 3,
            "base_backoff": 1.0
        }
        
        jina_connector = JinaConnector(jina_config)
        jina_service = JinaEmbeddingsService(jina_connector, embedding_config)
        
        # Create hybrid search service
        hybrid_config = HybridSearchConfig(
            default_k_value=60,
            parallel_search=True,
            timeout_seconds=30
        )
        
        hybrid_service = HybridSearchService(
            search_engine=search_engine,
            search_service=search_service,
            embedding_service=jina_service,
            config=hybrid_config
        )
        
        # Test queries
        test_queries = [
            "machine learning algorithms",
            "deep neural networks",
            "transformer architecture",
            "computer vision",
            "natural language processing"
        ]
        
        results = []
        
        for query in test_queries:
            try:
                request = HybridSearchRequest(
                    query=query,
                    size=5,
                    k_value=60,
                    search_engine=search_engine
                )
                
                response = await hybrid_service.hybrid_search(request)
                
                result = {
                    "query": query,
                    "total_unique": response.total_unique,
                    "bm25_total": response.bm25_total,
                    "semantic_total": response.semantic_total,
                    "overlap_count": response.overlap_count,
                    "took_ms": response.took,
                    "top_result_score": response.results[0].rrf_score if response.results else 0
                }
                
                results.append(result)
                print(f"Query '{query}': {result['total_unique']} results, {result['took_ms']}ms")
                
            except Exception as e:
                print(f"Error with query '{query}': {e}")
                results.append({"query": query, "error": str(e)})
        
        # Cleanup
        await hybrid_service.close()
        
        return results
    
    results = asyncio.run(run_hybrid_searches())
    return results

def rrf_performance_analysis(ti, **kwargs):
    """
    Task 3: Analyze RRF performance with different k-values.
    """
    setup_info = ti.xcom_pull(task_ids='setup_services_task')
    search_engine = setup_info['search_engine']
    
    print("Analyzing RRF performance with different k-values...")
    
    async def analyze_rrf():
        # Setup services (similar to previous task)
        if search_engine == "elasticsearch":
            provider = CredentialFactory.get_provider(mode="elasticsearchlocal")
            config = provider.get_credentials()
            connector = ConnectorFactory.get_connector("elasticsearch", config)
            search_service = ElasticsearchService(connector, index_name="papers")
        else:
            provider = CredentialFactory.get_provider(mode="opensearchlocal")
            config = provider.get_credentials()
            connector = ConnectorFactory.get_connector("opensearch", config)
            search_service = OpenSearchService(connector, index_name="papers")
        
        jina_config = {
            "base_url": "https://api.jina.ai/v1",
            "api_key": "your-jina-api-key",
            "timeout_seconds": 30
        }
        
        embedding_config = {
            "model": "jina-embeddings-v3",
            "dimensions": 1024,
            "tasks": {"passage": "text-matching", "query": "text-matching"},
            "max_retries": 3,
            "base_backoff": 1.0
        }
        
        jina_connector = JinaConnector(jina_config)
        jina_service = JinaEmbeddingsService(jina_connector, embedding_config)
        
        # Test different k-values
        k_values = [20, 40, 60, 80, 100]
        test_query = "machine learning neural networks"
        
        analysis_results = []
        
        for k_value in k_values:
            try:
                hybrid_config = HybridSearchConfig(default_k_value=k_value)
                hybrid_service = HybridSearchService(
                    search_engine=search_engine,
                    search_service=search_service,
                    embedding_service=jina_service,
                    config=hybrid_config
                )
                
                request = HybridSearchRequest(
                    query=test_query,
                    size=10,
                    k_value=k_value,
                    search_engine=search_engine
                )
                
                response = await hybrid_service.hybrid_search(request)
                
                result = {
                    "k_value": k_value,
                    "total_unique": response.total_unique,
                    "overlap_count": response.overlap_count,
                    "avg_rrf_score": sum(r.rrf_score for r in response.results) / len(response.results) if response.results else 0,
                    "took_ms": response.took
                }
                
                analysis_results.append(result)
                print(f"k={k_value}: {result['total_unique']} results, avg_score={result['avg_rrf_score']:.4f}")
                
                await hybrid_service.close()
                
            except Exception as e:
                print(f"Error with k={k_value}: {e}")
                analysis_results.append({"k_value": k_value, "error": str(e)})
        
        # Cleanup
        await search_service.close()
        
        return analysis_results
    
    results = asyncio.run(analyze_rrf())
    return results

def hybrid_search_monitoring(ti, **kwargs):
    """
    Task 4: Monitor hybrid search system performance and generate report.
    """
    setup_info = ti.xcom_pull(task_ids='setup_services_task')
    search_results = ti.xcom_pull(task_ids='hybrid_search_test_task')
    rrf_analysis = ti.xcom_pull(task_ids='rrf_analysis_task')
    
    print("Generating hybrid search monitoring report...")
    
    # Calculate statistics
    successful_searches = [r for r in search_results if 'error' not in r]
    failed_searches = [r for r in search_results if 'error' in r]
    
    if successful_searches:
        avg_response_time = sum(r['took_ms'] for r in successful_searches) / len(successful_searches)
        avg_unique_results = sum(r['total_unique'] for r in successful_searches) / len(successful_searches)
        avg_overlap = sum(r['overlap_count'] for r in successful_searches) / len(successful_searches)
    else:
        avg_response_time = avg_unique_results = avg_overlap = 0
    
    # Generate report
    report = {
        "timestamp": datetime.now().isoformat(),
        "search_engine": setup_info['search_engine'],
        "search_engine_health": setup_info['search_health'],
        "total_test_queries": len(search_results),
        "successful_searches": len(successful_searches),
        "failed_searches": len(failed_searches),
        "avg_response_time_ms": round(avg_response_time, 2),
        "avg_unique_results": round(avg_unique_results, 2),
        "avg_overlap_count": round(avg_overlap, 2),
        "rrf_analysis": rrf_analysis,
        "search_details": search_results
    }
    
    print("=== Hybrid Search Monitoring Report ===")
    print(f"Search Engine: {report['search_engine']}")
    print(f"Engine Health: {report['search_engine_health']}")
    print(f"Successful Searches: {report['successful_searches']}/{report['total_test_queries']}")
    print(f"Average Response Time: {report['avg_response_time_ms']}ms")
    print(f"Average Unique Results: {report['avg_unique_results']}")
    print(f"Average Overlap: {report['avg_overlap_count']}")
    
    if failed_searches:
        print(f"Failed Searches: {len(failed_searches)}")
        for failure in failed_searches:
            print(f"  - {failure['query']}: {failure['error']}")
    
    return report

# DAG configuration
default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Create DAG
with DAG(
    'hybrid_search_rrf_pipeline',
    default_args=default_args,
    description='RRF Hybrid Search operations and monitoring pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["hybrid-search", "rrf", "monitoring"],
    max_active_runs=1,
    params={
        "search_engine": "elasticsearch"  # Can be overridden at runtime
    }
) as dag:

    # Task 1: Setup services
    setup_services_task = PythonOperator(
        task_id='setup_services_task',
        python_callable=setup_hybrid_search_services,
        doc_md="Setup and validate hybrid search services"
    )

    # Task 2: Hybrid search tests
    hybrid_search_test_task = PythonOperator(
        task_id='hybrid_search_test_task',
        python_callable=perform_hybrid_search_test,
        doc_md="Perform hybrid search tests with multiple queries"
    )

    # Task 3: RRF analysis
    rrf_analysis_task = PythonOperator(
        task_id='rrf_analysis_task',
        python_callable=rrf_performance_analysis,
        doc_md="Analyze RRF performance with different k-values"
    )

    # Task 4: Monitoring and reporting
    monitoring_task = PythonOperator(
        task_id='hybrid_search_monitoring_task',
        python_callable=hybrid_search_monitoring,
        doc_md="Generate hybrid search monitoring report"
    )

    # Define task dependencies
    setup_services_task >> [hybrid_search_test_task, rrf_analysis_task] >> monitoring_task
