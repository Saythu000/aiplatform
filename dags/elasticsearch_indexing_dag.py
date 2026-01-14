from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import asyncio
import logging

from src.custom.credentials.factory import CredentialFactory
from src.custom.connectors.factory import ConnectorFactory
from src.custom.search.elasticsearch_service import ElasticsearchService
from src.custom.search.schemas.search_schemas import IndexRequest

logger = logging.getLogger(__name__)

def get_elasticsearch_credentials(**kwargs):
    """
    Task 1: Fetch Elasticsearch credentials.
    """
    print("Fetching Elasticsearch credentials...")
    provider = CredentialFactory.get_provider(mode="elasticsearchlocal")
    config = provider.get_credentials()
    print(f"Elasticsearch config loaded: {config['host']}:{config['port']}")
    return config

def elasticsearch_health_check(ti, **kwargs):
    """
    Task 2: Check Elasticsearch cluster health.
    """
    config = ti.xcom_pull(task_ids='elasticsearch_credentials_task')
    
    if not config:
        raise ValueError("No Elasticsearch configuration found in XCom!")

    async def check_health():
        connector = ConnectorFactory.get_connector("elasticsearch", config)
        service = ElasticsearchService(connector, index_name="papers")
        
        health = await service.health_check()
        await service.close()
        
        print(f"Elasticsearch cluster health: {health.status}")
        if health.status == "red":
            raise RuntimeError("Elasticsearch cluster is unhealthy!")
        
        return health.status
    
    # Run async function
    result = asyncio.run(check_health())
    return result

def create_elasticsearch_index(ti, **kwargs):
    """
    Task 3: Create Elasticsearch index with proper mappings.
    """
    config = ti.xcom_pull(task_ids='elasticsearch_credentials_task')
    
    async def create_index():
        connector = ConnectorFactory.get_connector("elasticsearch", config)
        service = ElasticsearchService(connector, index_name="papers")
        
        success = await service.create_index()
        await service.close()
        
        if success:
            print("Elasticsearch index created successfully!")
        else:
            print("Elasticsearch index already exists or creation failed")
        
        return success
    
    result = asyncio.run(create_index())
    return result

def index_sample_papers(ti, **kwargs):
    """
    Task 4: Index sample papers from existing data.
    """
    config = ti.xcom_pull(task_ids='elasticsearch_credentials_task')
    
    async def index_papers():
        connector = ConnectorFactory.get_connector("elasticsearch", config)
        service = ElasticsearchService(connector, index_name="papers")
        
        # Sample papers (replace with your actual data source)
        sample_papers = [
            IndexRequest(
                arxiv_id="2512.25072v1",
                title="Sample Paper 3: Transformer Architecture",
                authors="Emily Chen, David Brown",
                abstract="This paper presents novel transformer architectures...",
                content="Detailed analysis of transformer models and attention mechanisms...",
                published_date="2024-12-25",
                pdf_url="https://arxiv.org/pdf/2512.25072v1.pdf"
            ),
            IndexRequest(
                arxiv_id="2512.25055v1",
                title="Sample Paper 4: Computer Vision Applications",
                authors="Sarah Davis, Michael Lee",
                abstract="Exploring computer vision applications in real-world scenarios...",
                content="Comprehensive study of computer vision techniques...",
                published_date="2024-12-25",
                pdf_url="https://arxiv.org/pdf/2512.25055v1.pdf"
            ),
            IndexRequest(
                arxiv_id="2512.25075v1",
                title="Sample Paper 5: Natural Language Processing",
                authors="Robert Taylor, Lisa Anderson",
                abstract="Advanced natural language processing techniques and applications...",
                content="In-depth exploration of NLP methods and their implementations...",
                published_date="2024-12-25",
                pdf_url="https://arxiv.org/pdf/2512.25075v1.pdf"
            )
        ]
        
        # Bulk index papers
        result = await service.index_papers_bulk(sample_papers)
        await service.close()
        
        print(f"Indexed {result['indexed']} papers, {result['errors']} errors")
        return result
    
    result = asyncio.run(index_papers())
    return result

def elasticsearch_monitoring(ti, **kwargs):
    """
    Task 5: Monitor Elasticsearch performance and statistics.
    """
    config = ti.xcom_pull(task_ids='elasticsearch_credentials_task')
    
    async def monitor():
        connector = ConnectorFactory.get_connector("elasticsearch", config)
        service = ElasticsearchService(connector, index_name="papers")
        
        # Get cluster health
        health = await service.health_check()
        
        # Perform test search
        from src.custom.search.schemas.search_schemas import SearchRequest
        test_search = SearchRequest(query="transformer", size=5)
        search_result = await service.search_papers(test_search)
        
        await service.close()
        
        stats = {
            "cluster_health": health.status,
            "total_documents": search_result.total,
            "search_time": search_result.took,
            "cluster_name": health.cluster_name,
            "number_of_nodes": health.number_of_nodes
        }
        
        print(f"Elasticsearch monitoring stats: {stats}")
        return stats
    
    result = asyncio.run(monitor())
    return result

def elasticsearch_maintenance(ti, **kwargs):
    """
    Task 6: Perform Elasticsearch maintenance tasks.
    """
    config = ti.xcom_pull(task_ids='elasticsearch_credentials_task')
    
    async def maintenance():
        connector = ConnectorFactory.get_connector("elasticsearch", config)
        client = await connector.connect()
        
        # Force merge (optimize) index
        try:
            await client.indices.forcemerge(index="papers", max_num_segments=1)
            print("Index optimization completed")
        except Exception as e:
            print(f"Index optimization failed: {e}")
        
        # Refresh index
        try:
            await client.indices.refresh(index="papers")
            print("Index refresh completed")
        except Exception as e:
            print(f"Index refresh failed: {e}")
        
        await connector.close()
        return "Maintenance completed"
    
    result = asyncio.run(maintenance())
    return result

# DAG configuration
default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
}

# Create DAG
with DAG(
    'elasticsearch_indexing_pipeline',
    default_args=default_args,
    description='Elasticsearch indexing and management pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["elasticsearch", "indexing", "search"],
    max_active_runs=1,
) as dag:

    # Task 1: Get credentials
    elasticsearch_credentials_task = PythonOperator(
        task_id='elasticsearch_credentials_task',
        python_callable=get_elasticsearch_credentials,
        doc_md="Fetch Elasticsearch credentials from configuration"
    )

    # Task 2: Health check
    health_check_task = PythonOperator(
        task_id='elasticsearch_health_check_task',
        python_callable=elasticsearch_health_check,
        doc_md="Check Elasticsearch cluster health status"
    )

    # Task 3: Create index
    create_index_task = PythonOperator(
        task_id='create_elasticsearch_index_task',
        python_callable=create_elasticsearch_index,
        doc_md="Create Elasticsearch index with proper mappings"
    )

    # Task 4: Index papers
    index_papers_task = PythonOperator(
        task_id='index_papers_task',
        python_callable=index_sample_papers,
        doc_md="Index sample papers into Elasticsearch"
    )

    # Task 5: Monitoring
    monitoring_task = PythonOperator(
        task_id='elasticsearch_monitoring_task',
        python_callable=elasticsearch_monitoring,
        doc_md="Monitor Elasticsearch performance and statistics"
    )

    # Task 6: Maintenance
    maintenance_task = PythonOperator(
        task_id='elasticsearch_maintenance_task',
        python_callable=elasticsearch_maintenance,
        doc_md="Perform Elasticsearch maintenance and optimization"
    )

    # Define task dependencies
    elasticsearch_credentials_task >> health_check_task >> create_index_task >> index_papers_task >> [monitoring_task, maintenance_task]
