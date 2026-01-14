from setuptools import setup, find_packages

setup(
    name="aiplatform",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "apache-airflow",
        "docling",
        "pypdfium2",
        "httpx",
        "pydantic",
        "pendulum",
        "opensearch-py[async]",
        "elasticsearch[async]",
    ],
    python_requires=">=3.8",
)
