"""
Batch ETL Pipeline DAG
תהליך ETL יומי לנתוני נדל"ן

This DAG orchestrates the daily batch ETL pipeline:
1. Extract data from Gov.il API
2. Store raw data in Minio (S3)
3. Transform and normalize addresses
4. Load to PostgreSQL data warehouse
5. Run data quality checks
6. Refresh analytics metrics
"""

from datetime import datetime, timedelta
import logging
import sys
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

logger = logging.getLogger(__name__)

# ============================================
# DAG Configuration
# ============================================

default_args = {
    'owner': 'real-estate-roi',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ============================================
# Task Definitions
# ============================================

@task()
def extract_gov_data(days_back: int = 30):
    """
    Extract real estate transactions from Gov.il API.

    Args:
        days_back: Number of days to look back for transactions

    Returns:
        Dictionary with extraction results
    """
    from extract.gov_api import GovIlApiClient, parse_transaction_record

    logger.info(f"Starting extraction for last {days_back} days")

    client = GovIlApiClient()
    raw_records = client.get_recent_transactions(days=days_back)

    # Parse records
    parsed_records = [parse_transaction_record(r) for r in raw_records]

    logger.info(f"Extracted and parsed {len(parsed_records)} records")

    return {
        "record_count": len(parsed_records),
        "raw_records": raw_records,
        "parsed_records": parsed_records
    }


@task()
def store_raw_to_minio(extraction_result: dict):
    """
    Store raw extracted data to Minio S3 storage.

    Args:
        extraction_result: Output from extract_gov_data task

    Returns:
        Path where data was stored
    """
    from load.minio_client import MinioStorageClient

    logger.info("Storing raw data to Minio...")

    client = MinioStorageClient()

    # Store raw JSON
    raw_path = client.upload_json(
        data=extraction_result["raw_records"],
        path="raw/gov_transactions/transactions.json"
    )

    # Store parsed JSON
    parsed_path = client.upload_json(
        data=extraction_result["parsed_records"],
        path="raw/gov_transactions/parsed_transactions.json"
    )

    logger.info(f"Stored raw data to: {raw_path}")
    logger.info(f"Stored parsed data to: {parsed_path}")

    return {
        "raw_path": raw_path,
        "parsed_path": parsed_path,
        "record_count": extraction_result["record_count"],
        "parsed_records": extraction_result["parsed_records"]
    }


@task()
def transform_addresses(minio_result: dict):
    """
    Transform and normalize Hebrew addresses.

    Args:
        minio_result: Output from store_raw_to_minio task

    Returns:
        List of records with normalized addresses
    """
    from transform.address_normalizer import normalize_address_batch

    logger.info("Normalizing addresses...")

    records = minio_result["parsed_records"]
    normalized_records = normalize_address_batch(records)

    logger.info(f"Normalized {len(normalized_records)} addresses")

    return normalized_records


@task()
def load_to_warehouse(normalized_records: list):
    """
    Load transformed data to PostgreSQL data warehouse.

    Args:
        normalized_records: Records with normalized addresses

    Returns:
        Load statistics
    """
    from load.postgres_loader import PostgresLoader

    logger.info("Loading to data warehouse...")

    loader = PostgresLoader()
    stats = loader.load_transactions_batch(normalized_records)

    logger.info(f"Load complete: {stats}")

    return stats


@task()
def run_quality_checks(load_stats: dict):
    """
    Run data quality checks using SODA.

    Args:
        load_stats: Statistics from load task

    Returns:
        Quality check results
    """
    logger.info("Running data quality checks...")

    # Basic validation checks
    checks_passed = True
    messages = []

    if load_stats["errors"] > load_stats["total"] * 0.1:
        checks_passed = False
        messages.append(f"Error rate too high: {load_stats['errors']}/{load_stats['total']}")

    if load_stats["inserted"] == 0 and load_stats["total"] > 0:
        checks_passed = False
        messages.append("No records were inserted")

    result = {
        "passed": checks_passed,
        "messages": messages,
        "load_stats": load_stats
    }

    if not checks_passed:
        logger.warning(f"Quality checks failed: {messages}")
    else:
        logger.info("Quality checks passed")

    return result


@task()
def refresh_analytics(quality_result: dict):
    """
    Refresh ROI analytics metrics.

    Args:
        quality_result: Results from quality checks

    Returns:
        Number of metrics updated
    """
    from load.postgres_loader import PostgresLoader

    if not quality_result["passed"]:
        logger.warning("Skipping analytics refresh due to quality check failures")
        return {"skipped": True, "reason": quality_result["messages"]}

    logger.info("Refreshing analytics metrics...")

    loader = PostgresLoader()
    updated_count = loader.refresh_roi_metrics()

    logger.info(f"Updated {updated_count} location metrics")

    return {"updated_count": updated_count}


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='batch_etl_pipeline',
    default_args=default_args,
    description='Daily ETL pipeline for real estate transactions',
    schedule_interval='0 6 * * *',  # Run daily at 6:00 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['real-estate', 'etl', 'batch'],
    doc_md="""
    ## Batch ETL Pipeline
    ### תהליך ETL יומי לנתוני נדל"ן

    This DAG runs daily and performs the following steps:

    1. **Extract**: Fetch recent transactions from Gov.il API
    2. **Store Raw**: Save raw data to Minio (S3) with date partitioning
    3. **Transform**: Normalize Hebrew addresses
    4. **Load**: Insert/update data in PostgreSQL warehouse
    5. **Quality**: Run data validation checks
    6. **Analytics**: Refresh ROI metrics

    ### Configuration
    - Schedule: Daily at 06:00
    - Lookback: 30 days
    - Retries: 2

    ### Monitoring
    Check Minio console at http://localhost:9001 for raw data.
    Check PostgreSQL for loaded data.
    """
) as dag:

    # Define task flow
    extraction = extract_gov_data(days_back=30)
    raw_storage = store_raw_to_minio(extraction)
    transformed = transform_addresses(raw_storage)
    loaded = load_to_warehouse(transformed)
    quality = run_quality_checks(loaded)
    analytics = refresh_analytics(quality)

    # Task dependencies are automatically set by TaskFlow API
    # extraction >> raw_storage >> transformed >> loaded >> quality >> analytics


# ============================================
# Manual trigger function for testing
# ============================================

def run_manual():
    """Run the pipeline manually for testing."""
    logging.basicConfig(level=logging.INFO)

    print("=== Manual Pipeline Run ===\n")

    # Extract
    print("1. Extracting data...")
    from extract.gov_api import GovIlApiClient, parse_transaction_record
    client = GovIlApiClient()
    raw_records = client.fetch_transactions(limit=10)["records"]
    parsed = [parse_transaction_record(r) for r in raw_records]
    print(f"   Extracted {len(parsed)} records")

    # Transform
    print("\n2. Transforming addresses...")
    from transform.address_normalizer import normalize_address_batch
    normalized = normalize_address_batch(parsed)
    print(f"   Normalized {len(normalized)} records")

    # Show sample
    print("\n3. Sample record:")
    if normalized:
        sample = normalized[0]
        print(f"   City: {sample.get('normalized_city')}")
        print(f"   Street: {sample.get('normalized_street')}")
        print(f"   Price: {sample.get('price'):,.0f} ₪" if sample.get('price') else "   Price: N/A")

    print("\n=== Manual Run Complete ===")


if __name__ == "__main__":
    run_manual()
