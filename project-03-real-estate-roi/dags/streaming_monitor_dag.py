"""
Streaming Monitor DAG
DAG לניטור ועיבוד נתוני streaming מ-Kafka

This DAG monitors Kafka streaming health and processes accumulated messages:
1. Check Kafka cluster health
2. Check consumer lag
3. Process any accumulated messages
4. Update real-time metrics
5. Generate alerts for anomalies
"""

from datetime import datetime, timedelta
import logging
import sys
import os

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
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
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Thresholds for monitoring
MAX_CONSUMER_LAG = 1000  # Alert if lag exceeds this
MAX_PROCESSING_TIME_SECONDS = 300  # 5 minutes max processing time


# ============================================
# Task Definitions
# ============================================

@task()
def check_kafka_health():
    """
    Check Kafka cluster health and topic status.

    Returns:
        Health check results
    """
    from kafka import KafkaAdminClient
    from kafka.errors import KafkaError

    bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var="kafka:9092")

    results = {
        "healthy": False,
        "topics": [],
        "error": None,
        "timestamp": datetime.now().isoformat()
    }

    try:
        admin = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='airflow-health-check'
        )

        # List topics
        topics = admin.list_topics()
        results["topics"] = list(topics)
        results["healthy"] = True

        # Check our required topics exist
        required_topics = ["new_listings", "price_updates", "alerts"]
        missing = [t for t in required_topics if t not in topics]

        if missing:
            results["warning"] = f"Missing topics: {missing}"
            logger.warning(f"Missing Kafka topics: {missing}")

        admin.close()
        logger.info(f"Kafka health check passed. Topics: {results['topics']}")

    except KafkaError as e:
        results["error"] = str(e)
        logger.error(f"Kafka health check failed: {e}")

    except Exception as e:
        results["error"] = str(e)
        logger.error(f"Unexpected error in health check: {e}")

    return results


@task()
def check_consumer_lag(health_result: dict):
    """
    Check consumer group lag for all partitions.

    Args:
        health_result: Output from health check task

    Returns:
        Lag information and alerts
    """
    from kafka import KafkaConsumer

    if not health_result.get("healthy"):
        logger.warning("Skipping lag check - Kafka unhealthy")
        return {"skipped": True, "reason": "Kafka unhealthy"}

    bootstrap_servers = Variable.get("KAFKA_BOOTSTRAP_SERVERS", default_var="kafka:9092")
    consumer_group = "real-estate-processors"

    results = {
        "total_lag": 0,
        "partition_lag": {},
        "alerts": [],
        "timestamp": datetime.now().isoformat()
    }

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=consumer_group,
            enable_auto_commit=False
        )

        # Subscribe to get assignments
        consumer.subscribe(["new_listings"])
        consumer.poll(timeout_ms=5000)  # Trigger assignment

        # Get lag for each partition
        for tp in consumer.assignment():
            position = consumer.position(tp)
            end_offsets = consumer.end_offsets([tp])
            end_offset = end_offsets.get(tp, 0)

            lag = max(0, end_offset - position)
            partition_key = f"{tp.topic}-{tp.partition}"
            results["partition_lag"][partition_key] = lag
            results["total_lag"] += lag

            if lag > MAX_CONSUMER_LAG:
                alert = f"High lag on {partition_key}: {lag} messages"
                results["alerts"].append(alert)
                logger.warning(alert)

        consumer.close()

        logger.info(f"Consumer lag check: total_lag={results['total_lag']}")

    except Exception as e:
        logger.error(f"Error checking consumer lag: {e}")
        results["error"] = str(e)

    return results


@task()
def process_accumulated_messages(lag_result: dict):
    """
    Process any accumulated messages in Kafka.

    Args:
        lag_result: Output from lag check task

    Returns:
        Processing statistics
    """
    if lag_result.get("skipped"):
        return {"skipped": True, "reason": lag_result.get("reason")}

    total_lag = lag_result.get("total_lag", 0)

    if total_lag == 0:
        logger.info("No messages to process")
        return {"processed": 0, "message": "No pending messages"}

    # Import consumer
    from extract.kafka_consumer import RealEstateConsumer, default_listing_handler

    logger.info(f"Processing {total_lag} accumulated messages...")

    consumer = RealEstateConsumer(
        topics=["new_listings"],
        group_id="real-estate-processors-batch"  # Separate group for batch processing
    )

    # Process messages with timeout
    max_messages = min(total_lag, 1000)  # Process max 1000 at a time

    stats = consumer.process_messages(
        handler=default_listing_handler,
        max_messages=max_messages,
        timeout_ms=MAX_PROCESSING_TIME_SECONDS * 1000
    )

    results = stats.to_dict()
    logger.info(f"Processed {results['messages_processed']} messages")

    return results


@task()
def update_streaming_metrics(process_result: dict):
    """
    Update streaming metrics in the database.

    Args:
        process_result: Output from message processing

    Returns:
        Metrics update results
    """
    if process_result.get("skipped"):
        return process_result

    # Calculate metrics
    metrics = {
        "messages_processed": process_result.get("messages_processed", 0),
        "messages_failed": process_result.get("messages_failed", 0),
        "processing_time": process_result.get("uptime_seconds", 0),
        "timestamp": datetime.now().isoformat()
    }

    # In production, would save to a metrics table
    logger.info(f"Streaming metrics: {metrics}")

    return metrics


@task()
def generate_alerts(lag_result: dict, metrics_result: dict):
    """
    Generate alerts based on monitoring results.

    Args:
        lag_result: Consumer lag results
        metrics_result: Processing metrics

    Returns:
        List of generated alerts
    """
    alerts = []

    # Check for high lag alerts
    if lag_result.get("alerts"):
        alerts.extend(lag_result["alerts"])

    # Check for high failure rate
    processed = metrics_result.get("messages_processed", 0)
    failed = metrics_result.get("messages_failed", 0)

    if processed > 0:
        failure_rate = failed / (processed + failed)
        if failure_rate > 0.1:  # More than 10% failure
            alert = f"High message failure rate: {failure_rate:.1%}"
            alerts.append(alert)
            logger.warning(alert)

    if alerts:
        logger.warning(f"Generated {len(alerts)} alerts: {alerts}")

        # In production, would send to alerting system
        # e.g., Slack, PagerDuty, email

    return {"alerts": alerts, "alert_count": len(alerts)}


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='streaming_monitor',
    default_args=default_args,
    description='Monitor Kafka streaming and process messages',
    schedule_interval='*/15 * * * *',  # Every 15 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['real-estate', 'streaming', 'kafka', 'monitoring'],
    doc_md="""
    ## Streaming Monitor DAG
    ### ניטור Kafka ועיבוד הודעות

    This DAG runs every 15 minutes to:

    1. **Health Check**: Verify Kafka cluster is accessible
    2. **Lag Check**: Monitor consumer lag per partition
    3. **Process Messages**: Handle accumulated messages
    4. **Update Metrics**: Track streaming performance
    5. **Generate Alerts**: Alert on anomalies

    ### Thresholds
    - Max consumer lag: 1000 messages
    - Max processing time: 5 minutes
    - Failure rate alert: >10%

    ### Monitoring
    Check Kafka topics: `kafka-topics --list --bootstrap-server kafka:9092`
    Check consumer groups: `kafka-consumer-groups --list --bootstrap-server kafka:9092`
    """
) as dag:

    # Define task flow
    health = check_kafka_health()
    lag = check_consumer_lag(health)
    processed = process_accumulated_messages(lag)
    metrics = update_streaming_metrics(processed)
    alerts = generate_alerts(lag, metrics)


# ============================================
# Manual test function
# ============================================

def test_kafka_connection():
    """Test Kafka connection manually."""
    logging.basicConfig(level=logging.INFO)

    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")

    print(f"Testing Kafka connection to: {servers}")

    try:
        consumer = KafkaConsumer(
            bootstrap_servers=servers,
            consumer_timeout_ms=5000
        )
        topics = consumer.topics()
        print(f"Connected! Available topics: {topics}")
        consumer.close()
    except NoBrokersAvailable:
        print("Failed: No brokers available. Is Kafka running?")
    except Exception as e:
        print(f"Failed: {e}")


if __name__ == "__main__":
    test_kafka_connection()
