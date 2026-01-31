"""
Kafka Producer for Real Estate Listings
מודול לשליחת מודעות נדל"ן ל-Kafka

Produces simulated real estate listings to Kafka topics for real-time processing.
In production, this would be connected to a scraper or webhook.
"""

import json
import logging
import os
import random
import time
from datetime import datetime
from typing import Optional, Dict, Any
from dataclasses import dataclass, asdict

from kafka import KafkaProducer
from kafka.errors import KafkaError

logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
LISTINGS_TOPIC = "new_listings"
PRICE_UPDATES_TOPIC = "price_updates"
ALERTS_TOPIC = "alerts"


@dataclass
class ListingEvent:
    """Real estate listing event structure."""
    listing_id: str
    source: str  # Madlan, Yad2
    event_type: str  # new, update, removed
    city: str
    neighborhood: Optional[str]
    street: Optional[str]
    house_number: Optional[str]
    property_type: str
    rooms: Optional[float]
    size_sqm: Optional[float]
    floor: Optional[int]
    asking_price: float
    price_per_sqm: Optional[float]
    is_rental: bool
    url: Optional[str]
    timestamp: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


class RealEstateProducer:
    """Kafka producer for real estate listing events."""

    def __init__(
        self,
        bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS,
        client_id: str = "real-estate-producer"
    ):
        """
        Initialize Kafka producer.

        Args:
            bootstrap_servers: Kafka broker addresses
            client_id: Producer client identifier
        """
        self.bootstrap_servers = bootstrap_servers
        self.producer = None
        self.client_id = client_id

    def connect(self) -> None:
        """Connect to Kafka cluster."""
        try:
            self.producer = KafkaProducer(
                bootstrap_servers=self.bootstrap_servers,
                client_id=self.client_id,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks='all',
                retries=3,
                max_in_flight_requests_per_connection=1
            )
            logger.info(f"Connected to Kafka: {self.bootstrap_servers}")
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            raise

    def close(self) -> None:
        """Close producer connection."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")

    def send_listing(self, listing: ListingEvent) -> bool:
        """
        Send a listing event to Kafka.

        Args:
            listing: ListingEvent to send

        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.connect()

        try:
            # Use city as partition key for locality
            key = listing.city

            future = self.producer.send(
                topic=LISTINGS_TOPIC,
                key=key,
                value=listing.to_dict()
            )

            # Wait for send to complete
            record_metadata = future.get(timeout=10)

            logger.debug(
                f"Sent listing {listing.listing_id} to {record_metadata.topic} "
                f"partition {record_metadata.partition} offset {record_metadata.offset}"
            )
            return True

        except KafkaError as e:
            logger.error(f"Failed to send listing: {e}")
            return False

    def send_price_update(
        self,
        listing_id: str,
        old_price: float,
        new_price: float,
        city: str
    ) -> bool:
        """
        Send a price update event.

        Args:
            listing_id: ID of the listing
            old_price: Previous price
            new_price: New price
            city: City for partitioning

        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.connect()

        try:
            event = {
                "listing_id": listing_id,
                "old_price": old_price,
                "new_price": new_price,
                "price_change_pct": ((new_price - old_price) / old_price) * 100,
                "city": city,
                "timestamp": datetime.now().isoformat()
            }

            future = self.producer.send(
                topic=PRICE_UPDATES_TOPIC,
                key=city,
                value=event
            )
            future.get(timeout=10)
            return True

        except KafkaError as e:
            logger.error(f"Failed to send price update: {e}")
            return False

    def send_alert(self, alert_type: str, message: str, data: dict) -> bool:
        """
        Send an alert event.

        Args:
            alert_type: Type of alert (high_roi, price_drop, etc.)
            message: Alert message
            data: Additional alert data

        Returns:
            True if sent successfully
        """
        if not self.producer:
            self.connect()

        try:
            event = {
                "alert_type": alert_type,
                "message": message,
                "data": data,
                "timestamp": datetime.now().isoformat()
            }

            future = self.producer.send(
                topic=ALERTS_TOPIC,
                value=event
            )
            future.get(timeout=10)
            return True

        except KafkaError as e:
            logger.error(f"Failed to send alert: {e}")
            return False


# ============================================
# Simulated Data Generator
# ============================================

# Sample data for simulation
CITIES = [
    ("תל אביב יפו", ["רמת אביב", "פלורנטין", "יפו", "צפון תל אביב", "לב העיר"]),
    ("ירושלים", ["רחביה", "בית הכרם", "קטמון", "גילה", "ארנונה"]),
    ("חיפה", ["כרמל", "נווה שאנן", "הדר", "עין הים", "דניה"]),
    ("רמת גן", ["מרכז העיר", "רמת חן", "תל בנימין", "גפן"]),
    ("הרצליה", ["הרצליה פיתוח", "מרכז", "נווה עמל"]),
    ("ראשון לציון", ["נחלת יהודה", "רמת אליהו", "נווה הדרים"]),
]

STREETS = [
    "הרצל", "רוטשילד", "בן גוריון", "דיזנגוף", "אלנבי",
    "ז'בוטינסקי", "ויצמן", "הנביאים", "יפו", "קינג ג'ורג"
]

PROPERTY_TYPES = ["דירה", "פנטהאוז", "דירת גן", "קוטג'", "דופלקס"]


def generate_simulated_listing() -> ListingEvent:
    """Generate a random simulated listing for testing."""
    city, neighborhoods = random.choice(CITIES)
    neighborhood = random.choice(neighborhoods)
    street = random.choice(STREETS)

    rooms = random.choice([2, 2.5, 3, 3.5, 4, 4.5, 5])
    size_sqm = int(rooms * random.uniform(20, 35))
    is_rental = random.random() < 0.3

    if is_rental:
        price = int(random.uniform(3000, 15000))
    else:
        price_per_sqm = random.uniform(25000, 80000)
        price = int(size_sqm * price_per_sqm)

    return ListingEvent(
        listing_id=f"{random.choice(['MAD', 'YAD'])}-{random.randint(100000, 999999)}",
        source=random.choice(["Madlan", "Yad2"]),
        event_type="new",
        city=city,
        neighborhood=neighborhood,
        street=street,
        house_number=str(random.randint(1, 150)),
        property_type=random.choice(PROPERTY_TYPES),
        rooms=rooms,
        size_sqm=size_sqm,
        floor=random.randint(0, 15),
        asking_price=price,
        price_per_sqm=price / size_sqm if not is_rental else None,
        is_rental=is_rental,
        url=f"https://example.com/listing/{random.randint(100000, 999999)}",
        timestamp=datetime.now().isoformat()
    )


def run_simulation(
    count: int = 100,
    delay_seconds: float = 1.0,
    bootstrap_servers: str = KAFKA_BOOTSTRAP_SERVERS
):
    """
    Run a simulation that produces random listings.

    Args:
        count: Number of listings to produce
        delay_seconds: Delay between messages
        bootstrap_servers: Kafka servers
    """
    producer = RealEstateProducer(bootstrap_servers=bootstrap_servers)
    producer.connect()

    try:
        for i in range(count):
            listing = generate_simulated_listing()
            success = producer.send_listing(listing)

            if success:
                logger.info(
                    f"[{i+1}/{count}] Sent: {listing.listing_id} - "
                    f"{listing.city}, {listing.property_type}, "
                    f"₪{listing.asking_price:,}"
                )
            else:
                logger.warning(f"[{i+1}/{count}] Failed to send listing")

            time.sleep(delay_seconds)

    finally:
        producer.close()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    print("=== Kafka Producer Simulation ===\n")
    print(f"Bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {LISTINGS_TOPIC}\n")

    # Run simulation
    run_simulation(count=10, delay_seconds=2.0)
