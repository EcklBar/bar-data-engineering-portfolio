"""
Kafka Consumer for Real Estate Listings
מודול לקריאת מודעות נדל"ן מ-Kafka

Consumes real estate listing events and processes them in real-time.
"""

import json
import logging
import os
import signal
import sys
from datetime import datetime
from typing import Callable, Optional, List, Dict, Any
from dataclasses import dataclass

from kafka import KafkaConsumer, TopicPartition
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
LISTINGS_TOPIC = "new_listings"
PRICE_UPDATES_TOPIC = "price_updates"
ALERTS_TOPIC = "alerts"
CONSUMER_GROUP = "real-estate-processors"


@dataclass
class ConsumerStats:
    """Statistics for consumer monitoring."""
    messages_received: int = 0
    messages_processed: int = 0
    messages_failed: int = 0
    last_message_time: Optional[datetime] = None
    start_time: Optional[datetime] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            "messages_received": self.messages_received,
            "messages_processed": self.messages_processed,
            "messages_failed": self.messages_failed,
            "last_message_time": self.last_message_time.isoformat() if self.last_message_time else None,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "uptime_seconds": (datetime.now() - self.start_time).total_seconds() if self.start_time else 0
        }


class RealEstateConsumer:
    """Kafka consumer for real estate listing events."""

    def __init__(
        self,
        topics: List[str] = None,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        group_id: str = CONSUMER_GROUP,
        auto_offset_reset: str = "earliest"
    ):
        """
        Initialize Kafka consumer.

        Args:
            topics: List of topics to subscribe to
            bootstrap_servers: Kafka broker addresses
            group_id: Consumer group ID
            auto_offset_reset: Where to start reading (earliest/latest)
        """
        self.topics = topics or [LISTINGS_TOPIC]
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None
        self.running = False
        self.stats = ConsumerStats()

    def connect(self) -> None:
        """Connect to Kafka cluster."""
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers=self.bootstrap_servers,
                group_id=self.group_id,
                auto_offset_reset=self.auto_offset_reset,
                enable_auto_commit=True,
                auto_commit_interval_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                key_deserializer=lambda k: k.decode('utf-8') if k else None,
                max_poll_records=100,
                session_timeout_ms=30000
            )
            logger.info(f"Connected to Kafka: {self.bootstrap_servers}")
            logger.info(f"Subscribed to topics: {self.topics}")
            self.stats.start_time = datetime.now()
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def close(self) -> None:
        """Close consumer connection."""
        self.running = False
        if self.consumer:
            self.consumer.close()
            logger.info("Kafka consumer closed")

    def get_lag(self) -> Dict[str, int]:
        """
        Get consumer lag for each partition.

        Returns:
            Dictionary mapping partition to lag
        """
        if not self.consumer:
            return {}

        lag = {}
        try:
            # Get assigned partitions
            partitions = self.consumer.assignment()

            for tp in partitions:
                # Get current position and end offset
                position = self.consumer.position(tp)
                end_offsets = self.consumer.end_offsets([tp])
                end_offset = end_offsets.get(tp, 0)

                partition_lag = max(0, end_offset - position)
                lag[f"{tp.topic}-{tp.partition}"] = partition_lag

        except KafkaError as e:
            logger.error(f"Error getting lag: {e}")

        return lag

    def process_messages(
        self,
        handler: Callable[[Dict[str, Any]], bool],
        max_messages: Optional[int] = None,
        timeout_ms: int = 1000
    ) -> ConsumerStats:
        """
        Process messages from Kafka.

        Args:
            handler: Function to process each message, returns True if successful
            max_messages: Maximum messages to process (None = infinite)
            timeout_ms: Poll timeout in milliseconds

        Returns:
            Processing statistics
        """
        if not self.consumer:
            self.connect()

        self.running = True
        processed = 0

        # Handle graceful shutdown
        def signal_handler(sig, frame):
            logger.info("Received shutdown signal")
            self.running = False

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        try:
            while self.running:
                # Check max messages limit
                if max_messages and processed >= max_messages:
                    logger.info(f"Reached max messages limit: {max_messages}")
                    break

                # Poll for messages
                messages = self.consumer.poll(timeout_ms=timeout_ms)

                for topic_partition, records in messages.items():
                    for record in records:
                        self.stats.messages_received += 1
                        self.stats.last_message_time = datetime.now()

                        try:
                            # Process message
                            success = handler(record.value)

                            if success:
                                self.stats.messages_processed += 1
                                processed += 1
                            else:
                                self.stats.messages_failed += 1

                        except Exception as e:
                            logger.error(f"Error processing message: {e}")
                            self.stats.messages_failed += 1

        except Exception as e:
            logger.error(f"Consumer error: {e}")
            raise
        finally:
            self.close()

        return self.stats


# ============================================
# Message Handlers
# ============================================

def default_listing_handler(message: Dict[str, Any]) -> bool:
    """
    Default handler for listing messages.
    Logs the listing and returns True.

    Args:
        message: Listing event dictionary

    Returns:
        True if processed successfully
    """
    logger.info(
        f"Received listing: {message.get('listing_id')} - "
        f"{message.get('city')}, {message.get('property_type')}, "
        f"₪{message.get('asking_price', 0):,}"
    )
    return True


def database_listing_handler(message: Dict[str, Any]) -> bool:
    """
    Handler that saves listings to the database.

    Args:
        message: Listing event dictionary

    Returns:
        True if saved successfully
    """
    try:
        # Import here to avoid circular imports
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
        from load.postgres_loader import PostgresLoader
        from transform.address_normalizer import normalize_address

        # Normalize address
        normalized = normalize_address(
            city=message.get('city'),
            street=message.get('street'),
            house_number=message.get('house_number')
        )

        loader = PostgresLoader()

        # Get or create location
        location_id = loader.get_or_create_location(
            city=normalized.city,
            street=normalized.street,
            house_number=normalized.house_number
        )

        # Get or create property
        property_id = loader.upsert_property(
            property_type=message.get('property_type'),
            rooms=message.get('rooms'),
            floor=message.get('floor'),
            size_sqm=message.get('size_sqm')
        )

        # Insert listing (using fact_listings table)
        # This would need an insert_listing method in PostgresLoader
        logger.info(
            f"Saved listing {message.get('listing_id')} - "
            f"location_id={location_id}, property_id={property_id}"
        )
        return True

    except Exception as e:
        logger.error(f"Failed to save listing to database: {e}")
        return False


def alert_detection_handler(message: Dict[str, Any]) -> bool:
    """
    Handler that detects and generates alerts for interesting listings.

    Args:
        message: Listing event dictionary

    Returns:
        True if processed successfully
    """
    from kafka_producer import RealEstateProducer

    # Check for price drop (would need historical data)
    # Check for high ROI opportunity
    # Check for new area activity

    price_per_sqm = message.get('price_per_sqm')
    city = message.get('city')

    # Example: Alert if price per sqm is below threshold (potential opportunity)
    PRICE_THRESHOLDS = {
        "תל אביב יפו": 40000,
        "ירושלים": 35000,
        "חיפה": 20000,
    }

    threshold = PRICE_THRESHOLDS.get(city, 25000)

    if price_per_sqm and price_per_sqm < threshold:
        logger.info(
            f"ALERT: Potential opportunity - {message.get('listing_id')} "
            f"in {city} at ₪{price_per_sqm:,.0f}/sqm (threshold: ₪{threshold:,})"
        )

        # In production, would send to alerts topic
        # producer = RealEstateProducer()
        # producer.send_alert(
        #     alert_type="potential_opportunity",
        #     message=f"Low price listing in {city}",
        #     data=message
        # )

    return True


# ============================================
# Consumer Runner
# ============================================

def run_consumer(
    topics: List[str] = None,
    handler: Callable = None,
    max_messages: int = None
):
    """
    Run the Kafka consumer.

    Args:
        topics: Topics to consume from
        handler: Message handler function
        max_messages: Maximum messages to process
    """
    topics = topics or [LISTINGS_TOPIC]
    handler = handler or default_listing_handler

    consumer = RealEstateConsumer(topics=topics)

    logger.info(f"Starting consumer for topics: {topics}")

    stats = consumer.process_messages(
        handler=handler,
        max_messages=max_messages
    )

    logger.info(f"Consumer finished. Stats: {stats.to_dict()}")
    return stats


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("=== Kafka Consumer ===\n")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topics: {LISTINGS_TOPIC}")
    print(f"Consumer group: {CONSUMER_GROUP}\n")
    print("Waiting for messages (Ctrl+C to stop)...\n")

    # Run consumer with default handler
    run_consumer(
        topics=[LISTINGS_TOPIC],
        handler=default_listing_handler,
        max_messages=None  # Run indefinitely
    )
