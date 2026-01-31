"""
Analytics Refresh DAG
DAG לחישוב וריענון מדדי ROI

This DAG calculates and refreshes analytics metrics:
1. Fetch OSM amenity data for distance calculations
2. Calculate ROI metrics for each location
3. Generate investment alerts
4. Update analytics tables
"""

from datetime import datetime, timedelta
import logging
import sys
import os

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

logger = logging.getLogger(__name__)

# ============================================
# DAG Configuration
# ============================================

default_args = {
    'owner': 'real-estate-roi',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# ============================================
# Task Definitions
# ============================================

@task()
def fetch_amenity_data():
    """
    Fetch amenity data from OpenStreetMap for major cities.

    Returns:
        Dictionary with amenity data by city
    """
    from extract.osm_api import OpenStreetMapClient, CITY_BBOXES

    logger.info("Fetching amenity data from OpenStreetMap...")

    client = OpenStreetMapClient()
    amenities_by_city = {}

    # Fetch for each major city
    for city, bbox in list(CITY_BBOXES.items())[:5]:  # Limit to 5 cities to avoid rate limits
        try:
            logger.info(f"Fetching amenities for {city}...")

            stations = client.get_train_stations(bbox=bbox)

            amenities_by_city[city] = {
                "train_stations": [s.to_dict() for s in stations],
                "station_count": len(stations)
            }

            logger.info(f"  Found {len(stations)} train stations")

        except Exception as e:
            logger.error(f"Error fetching amenities for {city}: {e}")
            amenities_by_city[city] = {"error": str(e)}

    return amenities_by_city


@task()
def fetch_transaction_data():
    """
    Fetch recent transaction data from the warehouse.

    Returns:
        Transaction data grouped by location
    """
    from load.postgres_loader import PostgresLoader

    logger.info("Fetching transaction data from warehouse...")

    loader = PostgresLoader()

    # Get recent transactions (last 6 months)
    query = """
    SELECT
        l.location_id,
        l.city,
        l.neighborhood,
        l.lat,
        l.lng,
        t.price,
        t.price_per_sqm,
        t.transaction_date
    FROM core.fact_transactions t
    JOIN core.dim_locations l ON t.location_id = l.location_id
    WHERE t.transaction_date >= CURRENT_DATE - INTERVAL '6 months'
      AND t.price_per_sqm IS NOT NULL
      AND t.price_per_sqm > 0
    ORDER BY l.city, l.neighborhood, t.transaction_date
    """

    try:
        df = loader.fetch_query(query)
        logger.info(f"Fetched {len(df)} transactions")

        # Group by city/neighborhood
        grouped = df.groupby(['city', 'neighborhood']).apply(
            lambda x: x.to_dict('records')
        ).to_dict()

        return {
            "transaction_count": len(df),
            "grouped_data": grouped
        }

    except Exception as e:
        logger.error(f"Error fetching transactions: {e}")
        return {"error": str(e), "transaction_count": 0}


@task()
def calculate_roi_metrics(amenity_data: dict, transaction_data: dict):
    """
    Calculate ROI metrics for each location.

    Args:
        amenity_data: OSM amenity data
        transaction_data: Transaction data from warehouse

    Returns:
        Calculated metrics
    """
    from transform.feature_engineering import ROICalculator
    from extract.osm_api import haversine_distance

    if transaction_data.get("error"):
        logger.warning("Skipping ROI calculation due to data fetch error")
        return {"skipped": True, "reason": transaction_data["error"]}

    logger.info("Calculating ROI metrics...")

    calculator = ROICalculator()
    grouped_data = transaction_data.get("grouped_data", {})

    metrics_list = []
    location_id = 0

    for (city, neighborhood), transactions in grouped_data.items():
        location_id += 1

        # Calculate distance to nearest train station
        amenity_distances = {}
        city_amenities = amenity_data.get(city, {})
        stations = city_amenities.get("train_stations", [])

        if transactions and stations:
            # Get location coordinates from first transaction
            lat = transactions[0].get("lat")
            lng = transactions[0].get("lng")

            if lat and lng:
                # Find nearest station
                min_dist = float('inf')
                for station in stations:
                    dist = haversine_distance(
                        lat, lng,
                        station.get("lat", 0),
                        station.get("lon", 0)
                    )
                    min_dist = min(min_dist, dist)

                if min_dist != float('inf'):
                    amenity_distances["dist_to_train_stations_km"] = round(min_dist, 2)

        # Calculate metrics
        metrics = calculator.calculate_full_metrics(
            location_id=location_id,
            city=city,
            neighborhood=neighborhood or "מרכז",
            current_transactions=transactions,
            historical_transactions=None,  # Would need historical data
            rental_data=None,  # Would need rental data
            amenity_distances=amenity_distances
        )

        metrics_list.append(metrics.to_dict())

    logger.info(f"Calculated metrics for {len(metrics_list)} locations")

    return {
        "metrics": metrics_list,
        "location_count": len(metrics_list)
    }


@task()
def save_metrics_to_warehouse(metrics_result: dict):
    """
    Save calculated metrics to the analytics schema.

    Args:
        metrics_result: Calculated ROI metrics

    Returns:
        Save statistics
    """
    from load.postgres_loader import PostgresLoader
    from sqlalchemy import text

    if metrics_result.get("skipped"):
        return metrics_result

    logger.info("Saving metrics to warehouse...")

    loader = PostgresLoader()
    metrics_list = metrics_result.get("metrics", [])

    saved_count = 0
    error_count = 0

    for metrics in metrics_list:
        try:
            query = """
            INSERT INTO analytics.agg_roi_metrics (
                city, neighborhood, calc_date,
                avg_price_sqm, median_price_sqm,
                transaction_count, price_change_1y,
                avg_rental_yield, roi_score
            ) VALUES (
                :city, :neighborhood, :calc_date,
                :avg_price_sqm, :median_price_sqm,
                :transaction_count, :price_change_1y,
                :rental_yield, :roi_score
            )
            ON CONFLICT (location_id, calc_date) DO UPDATE SET
                avg_price_sqm = EXCLUDED.avg_price_sqm,
                median_price_sqm = EXCLUDED.median_price_sqm,
                transaction_count = EXCLUDED.transaction_count,
                price_change_1y = EXCLUDED.price_change_1y,
                avg_rental_yield = EXCLUDED.avg_rental_yield,
                roi_score = EXCLUDED.roi_score
            """

            loader.execute_query(query, {
                "city": metrics["city"],
                "neighborhood": metrics["neighborhood"],
                "calc_date": datetime.now().date(),
                "avg_price_sqm": metrics["avg_price_sqm"],
                "median_price_sqm": metrics["median_price_sqm"],
                "transaction_count": metrics["transaction_count"],
                "price_change_1y": metrics["price_change_1y"],
                "rental_yield": metrics["rental_yield_annual"],
                "roi_score": metrics["roi_score"]
            })
            saved_count += 1

        except Exception as e:
            logger.error(f"Error saving metrics for {metrics['city']}: {e}")
            error_count += 1

    logger.info(f"Saved {saved_count} metrics, {error_count} errors")

    return {
        "saved": saved_count,
        "errors": error_count
    }


@task()
def generate_investment_alerts(metrics_result: dict):
    """
    Generate alerts for high-ROI opportunities.

    Args:
        metrics_result: Calculated ROI metrics

    Returns:
        Generated alerts
    """
    if metrics_result.get("skipped"):
        return {"skipped": True}

    logger.info("Generating investment alerts...")

    metrics_list = metrics_result.get("metrics", [])
    alerts = []

    for metrics in metrics_list:
        # Alert for high ROI score
        if metrics["roi_score"] >= 75:
            alert = {
                "type": "high_roi",
                "city": metrics["city"],
                "neighborhood": metrics["neighborhood"],
                "roi_score": metrics["roi_score"],
                "message": f"High ROI opportunity in {metrics['city']}, {metrics['neighborhood']} (Score: {metrics['roi_score']})"
            }
            alerts.append(alert)
            logger.info(f"ALERT: {alert['message']}")

        # Alert for price appreciation
        if metrics["price_change_1y"] and metrics["price_change_1y"] > 10:
            alert = {
                "type": "price_growth",
                "city": metrics["city"],
                "neighborhood": metrics["neighborhood"],
                "price_change": metrics["price_change_1y"],
                "message": f"Strong price growth in {metrics['city']}, {metrics['neighborhood']} ({metrics['price_change_1y']}% YoY)"
            }
            alerts.append(alert)

    logger.info(f"Generated {len(alerts)} investment alerts")

    return {
        "alerts": alerts,
        "alert_count": len(alerts)
    }


# ============================================
# DAG Definition
# ============================================

with DAG(
    dag_id='analytics_refresh',
    default_args=default_args,
    description='Calculate and refresh ROI analytics metrics',
    schedule_interval='0 8 * * *',  # Daily at 8:00 AM
    start_date=days_ago(1),
    catchup=False,
    tags=['real-estate', 'analytics', 'roi'],
    doc_md="""
    ## Analytics Refresh DAG
    ### חישוב וריענון מדדי ROI

    This DAG runs daily after the batch ETL to:

    1. **Fetch Amenities**: Get OSM data for distance calculations
    2. **Fetch Transactions**: Get recent transaction data
    3. **Calculate ROI**: Compute investment metrics
    4. **Save Metrics**: Update analytics tables
    5. **Generate Alerts**: Create alerts for opportunities

    ### Metrics Calculated
    - Average/Median price per sqm
    - Price change trends (1Y)
    - Rental yield
    - ROI Score (1-100)
    - Risk Score (1-100)
    - Opportunity Score (1-100)

    ### Schedule
    Runs at 08:00 daily (after batch ETL at 06:00)
    """
) as dag:

    # Define task flow
    amenities = fetch_amenity_data()
    transactions = fetch_transaction_data()
    metrics = calculate_roi_metrics(amenities, transactions)
    saved = save_metrics_to_warehouse(metrics)
    alerts = generate_investment_alerts(metrics)

    # Set dependencies
    [amenities, transactions] >> metrics >> [saved, alerts]


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print("Analytics DAG defined successfully")
