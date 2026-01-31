"""
PostgreSQL Data Warehouse Loader
מודול לטעינת נתונים למחסן הנתונים

Handles loading transformed data into PostgreSQL with PostGIS support.
"""

import logging
import os
from typing import Optional, List
from contextlib import contextmanager
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_HOST = os.getenv("POSTGRES_HOST", "localhost")
DEFAULT_PORT = os.getenv("POSTGRES_PORT", "5433")
DEFAULT_DB = os.getenv("POSTGRES_DB", "real_estate_dw")
DEFAULT_USER = os.getenv("POSTGRES_USER", "postgres")
DEFAULT_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")


def get_connection_string(
    host: str = DEFAULT_HOST,
    port: str = DEFAULT_PORT,
    database: str = DEFAULT_DB,
    user: str = DEFAULT_USER,
    password: str = DEFAULT_PASSWORD
) -> str:
    """Build PostgreSQL connection string."""
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class PostgresLoader:
    """Data warehouse loader for PostgreSQL with PostGIS."""

    def __init__(self, connection_string: Optional[str] = None):
        """
        Initialize PostgreSQL connection.

        Args:
            connection_string: SQLAlchemy connection string
        """
        self.connection_string = connection_string or get_connection_string()
        self.engine = create_engine(self.connection_string)
        self.Session = sessionmaker(bind=self.engine)

    @contextmanager
    def get_connection(self):
        """Get database connection context manager."""
        conn = self.engine.connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def get_session(self):
        """Get SQLAlchemy session context manager."""
        session = self.Session()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()

    def execute_query(self, query: str, params: Optional[dict] = None) -> None:
        """Execute a SQL query."""
        with self.get_connection() as conn:
            conn.execute(text(query), params or {})
            conn.commit()

    def fetch_query(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Execute query and return results as DataFrame."""
        with self.get_connection() as conn:
            return pd.read_sql(text(query), conn, params=params)

    # ============================================
    # Location Dimension Operations
    # ============================================

    def upsert_location(
        self,
        city: str,
        neighborhood: Optional[str] = None,
        street: Optional[str] = None,
        house_number: Optional[str] = None,
        lat: Optional[float] = None,
        lng: Optional[float] = None
    ) -> int:
        """
        Insert or update a location and return its ID.

        Args:
            city: City name
            neighborhood: Neighborhood name
            street: Street name
            house_number: House number
            lat: Latitude
            lng: Longitude

        Returns:
            location_id
        """
        query = """
        INSERT INTO core.dim_locations (city, neighborhood, street, house_number, lat, lng, geom)
        VALUES (
            :city,
            :neighborhood,
            :street,
            :house_number,
            :lat,
            :lng,
            CASE
                WHEN :lat IS NOT NULL AND :lng IS NOT NULL
                THEN ST_SetSRID(ST_MakePoint(:lng, :lat), 4326)
                ELSE NULL
            END
        )
        ON CONFLICT (city, neighborhood, street, house_number)
        DO UPDATE SET
            lat = COALESCE(EXCLUDED.lat, core.dim_locations.lat),
            lng = COALESCE(EXCLUDED.lng, core.dim_locations.lng),
            geom = COALESCE(EXCLUDED.geom, core.dim_locations.geom),
            updated_at = CURRENT_TIMESTAMP
        RETURNING location_id
        """

        with self.get_connection() as conn:
            result = conn.execute(text(query), {
                "city": city,
                "neighborhood": neighborhood,
                "street": street,
                "house_number": house_number,
                "lat": lat,
                "lng": lng
            })
            conn.commit()
            row = result.fetchone()
            return row[0] if row else None

    def get_or_create_location(self, city: str, street: str = None,
                               house_number: str = None) -> int:
        """Get existing location ID or create new one."""
        # First try to find existing
        query = """
        SELECT location_id FROM core.dim_locations
        WHERE city = :city
          AND COALESCE(street, '') = COALESCE(:street, '')
          AND COALESCE(house_number, '') = COALESCE(:house_number, '')
        LIMIT 1
        """

        with self.get_connection() as conn:
            result = conn.execute(text(query), {
                "city": city,
                "street": street,
                "house_number": house_number
            })
            row = result.fetchone()
            if row:
                return row[0]

        # Create new
        return self.upsert_location(city=city, street=street, house_number=house_number)

    # ============================================
    # Property Dimension Operations
    # ============================================

    def upsert_property(
        self,
        property_type: str,
        rooms: Optional[float] = None,
        floor: Optional[int] = None,
        size_sqm: Optional[float] = None,
        year_built: Optional[int] = None,
        total_floors: Optional[int] = None
    ) -> int:
        """
        Insert or update a property type and return its ID.

        Returns:
            property_id
        """
        query = """
        INSERT INTO core.dim_properties (property_type, rooms, floor, total_floors, size_sqm, year_built)
        VALUES (:property_type, :rooms, :floor, :total_floors, :size_sqm, :year_built)
        ON CONFLICT (property_type, rooms, floor, size_sqm, year_built)
        DO UPDATE SET property_type = EXCLUDED.property_type
        RETURNING property_id
        """

        with self.get_connection() as conn:
            result = conn.execute(text(query), {
                "property_type": property_type or "לא ידוע",
                "rooms": rooms,
                "floor": floor,
                "total_floors": total_floors,
                "size_sqm": size_sqm,
                "year_built": year_built
            })
            conn.commit()
            row = result.fetchone()
            return row[0] if row else None

    # ============================================
    # Transaction Fact Operations
    # ============================================

    def insert_transaction(
        self,
        location_id: int,
        property_id: int,
        transaction_date: datetime,
        price: float,
        price_per_sqm: Optional[float] = None,
        deal_nature: Optional[str] = None,
        source_id: Optional[str] = None
    ) -> Optional[int]:
        """
        Insert a transaction record.

        Returns:
            transaction_id or None if duplicate
        """
        query = """
        INSERT INTO core.fact_transactions
            (location_id, property_id, transaction_date, price, price_per_sqm, deal_nature, source_id)
        VALUES
            (:location_id, :property_id, :transaction_date, :price, :price_per_sqm, :deal_nature, :source_id)
        ON CONFLICT (source_id) DO NOTHING
        RETURNING transaction_id
        """

        with self.get_connection() as conn:
            result = conn.execute(text(query), {
                "location_id": location_id,
                "property_id": property_id,
                "transaction_date": transaction_date,
                "price": price,
                "price_per_sqm": price_per_sqm,
                "deal_nature": deal_nature,
                "source_id": source_id
            })
            conn.commit()
            row = result.fetchone()
            return row[0] if row else None

    def load_transactions_batch(self, records: List[dict]) -> dict:
        """
        Load a batch of parsed transaction records.

        Args:
            records: List of parsed transaction dictionaries

        Returns:
            Statistics dictionary with counts
        """
        stats = {
            "total": len(records),
            "inserted": 0,
            "skipped": 0,
            "errors": 0
        }

        for record in records:
            try:
                # Get or create location
                location_id = self.get_or_create_location(
                    city=record.get("normalized_city") or record.get("city", ""),
                    street=record.get("normalized_street") or record.get("street"),
                    house_number=record.get("normalized_house_number") or record.get("house_number")
                )

                if not location_id:
                    stats["errors"] += 1
                    continue

                # Get or create property
                property_id = self.upsert_property(
                    property_type=record.get("property_type"),
                    rooms=record.get("rooms"),
                    floor=record.get("floor"),
                    size_sqm=record.get("size_sqm"),
                    year_built=record.get("year_built"),
                    total_floors=record.get("total_floors")
                )

                if not property_id:
                    stats["errors"] += 1
                    continue

                # Insert transaction
                transaction_id = self.insert_transaction(
                    location_id=location_id,
                    property_id=property_id,
                    transaction_date=record.get("transaction_date"),
                    price=record.get("price"),
                    price_per_sqm=record.get("price_per_sqm"),
                    deal_nature=record.get("deal_nature"),
                    source_id=record.get("source_id")
                )

                if transaction_id:
                    stats["inserted"] += 1
                else:
                    stats["skipped"] += 1  # Duplicate

            except Exception as e:
                logger.error(f"Error loading record: {e}")
                stats["errors"] += 1

        logger.info(f"Batch load complete: {stats}")
        return stats

    # ============================================
    # Analytics Operations
    # ============================================

    def refresh_roi_metrics(self, calc_date: Optional[datetime] = None) -> int:
        """
        Calculate and update ROI metrics for all locations.

        Args:
            calc_date: Date for calculation (defaults to today)

        Returns:
            Number of locations updated
        """
        calc_date = calc_date or datetime.now().date()

        query = """
        INSERT INTO analytics.agg_roi_metrics (
            location_id, city, neighborhood, calc_date,
            avg_price_sqm, median_price_sqm, transaction_count,
            price_change_1y, roi_score
        )
        SELECT
            l.location_id,
            l.city,
            l.neighborhood,
            :calc_date as calc_date,
            AVG(t.price_per_sqm) as avg_price_sqm,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.price_per_sqm) as median_price_sqm,
            COUNT(*) as transaction_count,
            -- Calculate 1-year price change
            (AVG(t.price_per_sqm) - AVG(CASE WHEN t.transaction_date < :calc_date - INTERVAL '1 year' THEN t.price_per_sqm END))
            / NULLIF(AVG(CASE WHEN t.transaction_date < :calc_date - INTERVAL '1 year' THEN t.price_per_sqm END), 0) * 100
            as price_change_1y,
            -- Simple ROI score (normalized 1-100)
            LEAST(100, GREATEST(0,
                50 + (AVG(t.price_per_sqm) - 30000) / 1000  -- Baseline adjustment
            )) as roi_score
        FROM core.dim_locations l
        JOIN core.fact_transactions t ON l.location_id = t.location_id
        WHERE t.transaction_date >= :calc_date - INTERVAL '2 years'
          AND t.price_per_sqm IS NOT NULL
          AND t.price_per_sqm > 0
        GROUP BY l.location_id, l.city, l.neighborhood
        HAVING COUNT(*) >= 3
        ON CONFLICT (location_id, calc_date)
        DO UPDATE SET
            avg_price_sqm = EXCLUDED.avg_price_sqm,
            median_price_sqm = EXCLUDED.median_price_sqm,
            transaction_count = EXCLUDED.transaction_count,
            price_change_1y = EXCLUDED.price_change_1y,
            roi_score = EXCLUDED.roi_score,
            created_at = CURRENT_TIMESTAMP
        """

        with self.get_connection() as conn:
            result = conn.execute(text(query), {"calc_date": calc_date})
            conn.commit()
            return result.rowcount

    def get_top_opportunities(self, limit: int = 20) -> pd.DataFrame:
        """Get top investment opportunities by ROI score."""
        query = """
        SELECT
            city,
            neighborhood,
            roi_score,
            avg_price_sqm,
            price_change_1y,
            transaction_count,
            calc_date
        FROM analytics.agg_roi_metrics
        WHERE calc_date = (SELECT MAX(calc_date) FROM analytics.agg_roi_metrics)
        ORDER BY roi_score DESC
        LIMIT :limit
        """
        return self.fetch_query(query, {"limit": limit})


# Convenience functions for Airflow tasks
def load_transactions_to_warehouse(records: List[dict]) -> dict:
    """Load parsed transactions to data warehouse."""
    loader = PostgresLoader()
    return loader.load_transactions_batch(records)


def refresh_analytics() -> int:
    """Refresh ROI analytics metrics."""
    loader = PostgresLoader()
    return loader.refresh_roi_metrics()


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test connection
    loader = PostgresLoader()

    print("Testing PostgreSQL connection...")

    # Test query
    try:
        result = loader.fetch_query("SELECT PostGIS_Version()")
        print(f"PostGIS Version: {result.iloc[0, 0]}")
    except Exception as e:
        print(f"Connection test failed: {e}")
        print("Make sure PostgreSQL is running with: docker-compose up -d postgres-dw")
