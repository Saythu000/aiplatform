from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

from src.custom.connectors.factory import ConnectorFactory
from src.custom.extractors.factory import ExtractorFactory
from src.custom.transformers.factory import TransformerFactory
from src.custom.loaders.factory import LoaderFactory
from src.custom.utils.reader import load_yml

def extraction(**kwargs):
    """Extract papers from ArXiv API."""

    config_path = "dags/structure/arxiv/config/extractor.yml"
    full_yml = load_yml(config_path)

    extract_config = full_yml.get("arxiv", {}).get("extraction", {})

    # ArXiv doesn't need credentials, pass empty config
    connector = ConnectorFactory.get_connector(connector_type="arxiv", config={})
    connection = connector()

    try:
        extractor = ExtractorFactory.get_extractor(
            extractor_type="arxiv",
            connection=connection,
            config=extract_config
        )

        data_json = extractor()

        for table_name, rows in data_json.items():
            print(f"Extracted {len(rows)} records from {table_name}")

        return data_json

    finally:
        # Close connection if needed
        pass

def transformation(ti, **kwargs):
    """Transform ArXiv data."""
    raw_data = ti.xcom_pull(task_ids='extraction_task')

    if not raw_data:
        raise ValueError("No data received from extraction task!")

    # For now, just pass through the data
    # You can add transformation logic later
    transformer_config = {"index_name": "arxiv_papers"}

    transformer = TransformerFactory.get_transformer(
        transformer_type="json",
        data=raw_data,
        config=transformer_config
    )

    normalized_data = transformer()

    # Convert generator to list for XCom
    normalized_list = list(normalized_data)

    print(f"Successfully transformed {len(normalized_list)} records")
    return normalized_list

def loading(ti, **kwargs):
    """Load ArXiv data to Elasticsearch."""
    transformed_data = ti.xcom_pull(task_ids='transformation_task')

    if not transformed_data:
        raise ValueError("No transformed data found in XCom!")

    print(f"Would load {len(transformed_data)} ArXiv papers to Elasticsearch")
    # TODO: Add Elasticsearch loading when ready

default_args = {
    'owner': 'data_team',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    'arxiv_data_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["structure", "arxiv"]
) as dag:

    extraction_task = PythonOperator(
        task_id='extraction_task',
        python_callable=extraction,
    )

    transformation_task = PythonOperator(
        task_id='transformation_task',
        python_callable=transformation,
    )

    loading_task = PythonOperator(
        task_id='loading_task',
        python_callable=loading,
    )

    extraction_task >> transformation_task >> loading_task