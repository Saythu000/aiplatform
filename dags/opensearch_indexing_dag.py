from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import asyncio
import logging

from src.custom.credentials.factory import CredentialFactory
from src.custom.connectors.factory import ConnectorFactory
from src.custom.search.opensearch_service import OpenSearchService
from src.custom.search.schemas.search_schemas import IndexRequest

logger = logging.getLogger(__name__)

def get_opensearch_credentials(**kwargs):
    """
    Task 1: Fetch OpenSearch credentials.
    """
    print("Fetching OpenSearch credentials...")
    provider = CredentialFactory.get_provider(mode="opensearchlocal")
    config = provider.get_credentials()
    print(f"OpenSearch config loaded: {config['host']}:{config['port']}")
    return config

def opensearch_health_check(ti, **kwargs):
    """
    Task 2: Check OpenSearch cluster health.
    """
    config = ti.xcom_pull(task_ids='opensearch_credentials_task')
    
    if not config:
        raise ValueError("No OpenSearch configuration found in XCom!")

    async def check_health():
        connector = ConnectorFactory.get_connector("opensearch", config)
        service = OpenSearchService(connector, index_name="papers")
        
        health = await service.health_check()
        await service.close()
        
        print(f"OpenSearch cluster health: {health.status}")
        if health.status == "red":
            raise RuntimeError("OpenSearch cluster is unhealthy!")
        
        return health.status
    
    # Run async function
    result = asyncio.run(check_health())
    return result

def create_opensearch_index(ti, **kwargs):
    """
    Task 3: Create OpenSearch index with proper mappings.
    """
    config = ti.xcom_pull(task_ids='opensearch_credentials_task')
    
    async def create_index():
        connector = ConnectorFactory.get_connector("opensearch", config)
        service = OpenSearchService(connector, index_name="papers")
        
        success = await service.create_index()
        await service.close()
        
        if success:
            print("OpenSearch index created successfully!")
        else:
            print("OpenSearch index already exists or creation failed")
        
        return success
    
    result = asyncio.run(create_index())
    return result

def index_sample_papers(ti, **kwargs):
    """
    Task 4: Index sample papers from existing data.
    """
    config = ti.xcom_pull(task_ids='opensearch_credentials_task')
    
    async def index_papers():
        connector = ConnectorFactory.get_connector("opensearch", config)
        service = OpenSearchService(connector, index_name="papers")
        
        # Sample papers (replace with your actual data source)
        sample_papers = [
            IndexRequest(
                arxiv_id="2512.25052v1",
                title="Sample Paper 1: Machine Learning Advances",
                authors="John Doe, Jane Smith",
                abstract="This paper discusses recent advances in machine learning...",
                content="Full content of the paper would go here...",
                published_date="2024-12-25",
                pdf_url="https://arxiv.org/pdf/2512.25052v1.pdf"
            ),
            IndexRequest(
                arxiv_id="2512.25065v1",
                title="Sample Paper 2: Deep Neural Networks",
                authors="Alice Johnson, Bob Wilson",
                abstract="An exploration of deep neural network architectures...",
                content="Detailed content about neural networks...",
                published_date="2024-12-25",
                pdf_url="https://arxiv.org/pdf/2512.25065v1.pdf"
            )
        ]
        
        # Bulk index papers
        result = await service.index_papers_bulk(sample_papers)
        await service.close()
        
        print(f"Indexed {result['indexed']} papers, {result['errors']} errors")
        return result
    
    result = asyncio.run(index_papers())
    return result

def opensearch_monitoring(ti, **kwargs):
    """
    Task 5: Monitor OpenSearch performance and statistics.
    """
    config = ti.xcom_pull(task_ids='opensearch_credentials_task')
    
    async def monitor():
        connector = ConnectorFactory.get_connector("opensearch", config)
        service = OpenSearchService(connector, index_name="papers")
        
        # Get cluster health
        health = await service.health_check()
        
        # Perform test search
        from src.custom.search.schemas.search_schemas import SearchRequest
        test_search = SearchRequest(query="machine learning", size=5)
        search_result = await service.search_papers(test_search)
        
        await service.close()
        
        stats = {
            "cluster_health": health.status,
            "total_documents": search_result.total,
            "search_time": search_result.took
        }
        
        print(f"OpenSearch monitoring stats: {stats}")
        return stats
    
    result = asyncio.run(monitor())
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
    'opensearch_indexing_pipeline',
    default_args=default_args,
    description='OpenSearch indexing and management pipeline',
    schedule_interval='@daily',
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["opensearch", "indexing", "search"],
    max_active_runs=1,
) as dag:

    # Task 1: Get credentials
    opensearch_credentials_task = PythonOperator(
        task_id='opensearch_credentials_task',
        python_callable=get_opensearch_credentials,
        doc_md="Fetch OpenSearch credentials from configuration"
    )

    # Task 2: Health check
    health_check_task = PythonOperator(
        task_id='opensearch_health_check_task',
        python_callable=opensearch_health_check,
        doc_md="Check OpenSearch cluster health status"
    )

    # Task 3: Create index
    create_index_task = PythonOperator(
        task_id='create_opensearch_index_task',
        python_callable=create_opensearch_index,
        doc_md="Create OpenSearch index with proper mappings"
    )

    # Task 4: Index papers
    index_papers_task = PythonOperator(
        task_id='index_papers_task',
        python_callable=index_sample_papers,
        doc_md="Index sample papers into OpenSearch"
    )

    # Task 5: Monitoring
    monitoring_task = PythonOperator(
        task_id='opensearch_monitoring_task',
        python_callable=opensearch_monitoring,
        doc_md="Monitor OpenSearch performance and statistics"
    )

    # Define task dependencies
    opensearch_credentials_task >> health_check_task >> create_index_task >> index_papers_task >> monitoring_task
