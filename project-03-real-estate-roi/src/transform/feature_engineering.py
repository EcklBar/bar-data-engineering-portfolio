"""
Feature Engineering Module
מודול להנדסת פיצ'רים וחישוב ROI

Calculates investment metrics and ROI scores for real estate properties.
"""

import logging
from typing import Dict, List, Optional
from dataclasses import dataclass
from datetime import datetime, timedelta
import statistics

logger = logging.getLogger(__name__)


@dataclass
class ROIMetrics:
    """ROI and investment metrics for a location."""
    location_id: int
    city: str
    neighborhood: Optional[str]

    # Price metrics
    avg_price_sqm: float
    median_price_sqm: float
    min_price_sqm: float
    max_price_sqm: float
    price_std_dev: float
    transaction_count: int

    # Trend metrics
    price_change_1m: Optional[float]
    price_change_3m: Optional[float]
    price_change_1y: Optional[float]

    # Rental metrics
    avg_rental_price: Optional[float]
    rental_yield_annual: Optional[float]

    # Composite scores
    roi_score: float  # 1-100
    risk_score: float  # 1-100
    opportunity_score: float  # 1-100

    calc_date: datetime

    def to_dict(self) -> Dict:
        return {
            "location_id": self.location_id,
            "city": self.city,
            "neighborhood": self.neighborhood,
            "avg_price_sqm": self.avg_price_sqm,
            "median_price_sqm": self.median_price_sqm,
            "min_price_sqm": self.min_price_sqm,
            "max_price_sqm": self.max_price_sqm,
            "price_std_dev": self.price_std_dev,
            "transaction_count": self.transaction_count,
            "price_change_1m": self.price_change_1m,
            "price_change_3m": self.price_change_3m,
            "price_change_1y": self.price_change_1y,
            "avg_rental_price": self.avg_rental_price,
            "rental_yield_annual": self.rental_yield_annual,
            "roi_score": self.roi_score,
            "risk_score": self.risk_score,
            "opportunity_score": self.opportunity_score,
            "calc_date": self.calc_date.isoformat() if self.calc_date else None
        }


class ROICalculator:
    """Calculator for real estate ROI and investment metrics."""

    # Market benchmarks (these would be loaded from config/database in production)
    CITY_BENCHMARKS = {
        "תל אביב יפו": {"avg_price_sqm": 55000, "avg_yield": 2.5},
        "ירושלים": {"avg_price_sqm": 45000, "avg_yield": 3.0},
        "חיפה": {"avg_price_sqm": 25000, "avg_yield": 4.0},
        "באר שבע": {"avg_price_sqm": 18000, "avg_yield": 5.0},
        "ראשון לציון": {"avg_price_sqm": 35000, "avg_yield": 3.2},
        "פתח תקווה": {"avg_price_sqm": 32000, "avg_yield": 3.5},
        "נתניה": {"avg_price_sqm": 30000, "avg_yield": 3.8},
        "רמת גן": {"avg_price_sqm": 42000, "avg_yield": 3.0},
    }

    DEFAULT_BENCHMARK = {"avg_price_sqm": 30000, "avg_yield": 3.5}

    def __init__(self):
        pass

    def calculate_price_metrics(
        self,
        prices_per_sqm: List[float]
    ) -> Dict[str, float]:
        """
        Calculate price statistics from a list of prices.

        Args:
            prices_per_sqm: List of price per sqm values

        Returns:
            Dictionary with price metrics
        """
        if not prices_per_sqm:
            return {}

        # Filter out invalid values
        valid_prices = [p for p in prices_per_sqm if p and p > 0]

        if not valid_prices:
            return {}

        return {
            "avg_price_sqm": round(statistics.mean(valid_prices), 2),
            "median_price_sqm": round(statistics.median(valid_prices), 2),
            "min_price_sqm": round(min(valid_prices), 2),
            "max_price_sqm": round(max(valid_prices), 2),
            "price_std_dev": round(statistics.stdev(valid_prices), 2) if len(valid_prices) > 1 else 0,
            "transaction_count": len(valid_prices)
        }

    def calculate_price_change(
        self,
        current_prices: List[float],
        historical_prices: List[float]
    ) -> Optional[float]:
        """
        Calculate percentage price change.

        Args:
            current_prices: Recent price data
            historical_prices: Historical price data

        Returns:
            Percentage change or None
        """
        if not current_prices or not historical_prices:
            return None

        current_avg = statistics.mean(current_prices)
        historical_avg = statistics.mean(historical_prices)

        if historical_avg == 0:
            return None

        change = ((current_avg - historical_avg) / historical_avg) * 100
        return round(change, 2)

    def calculate_rental_yield(
        self,
        avg_price: float,
        monthly_rent: float
    ) -> Optional[float]:
        """
        Calculate annual rental yield percentage.

        Args:
            avg_price: Average property price
            monthly_rent: Average monthly rent

        Returns:
            Annual yield percentage
        """
        if not avg_price or not monthly_rent or avg_price == 0:
            return None

        annual_rent = monthly_rent * 12
        yield_pct = (annual_rent / avg_price) * 100
        return round(yield_pct, 2)

    def calculate_roi_score(
        self,
        city: str,
        avg_price_sqm: float,
        price_change_1y: Optional[float],
        rental_yield: Optional[float],
        dist_to_train: Optional[float] = None,
        transaction_count: int = 0
    ) -> float:
        """
        Calculate composite ROI score (1-100).

        Higher score = better investment potential.

        Args:
            city: City name
            avg_price_sqm: Average price per sqm
            price_change_1y: 1-year price change percentage
            rental_yield: Annual rental yield percentage
            dist_to_train: Distance to train station in km
            transaction_count: Number of transactions (liquidity indicator)

        Returns:
            ROI score between 1-100
        """
        benchmark = self.CITY_BENCHMARKS.get(city, self.DEFAULT_BENCHMARK)
        score = 50.0  # Start at neutral

        # Price relative to benchmark (lower = better value)
        if avg_price_sqm and benchmark["avg_price_sqm"]:
            price_ratio = avg_price_sqm / benchmark["avg_price_sqm"]
            if price_ratio < 0.8:
                score += 15  # Significantly below market
            elif price_ratio < 0.95:
                score += 8  # Below market
            elif price_ratio > 1.2:
                score -= 10  # Above market
            elif price_ratio > 1.05:
                score -= 5  # Slightly above market

        # Price appreciation trend
        if price_change_1y is not None:
            if price_change_1y > 10:
                score += 12  # Strong growth
            elif price_change_1y > 5:
                score += 8
            elif price_change_1y > 0:
                score += 4
            elif price_change_1y > -5:
                score -= 2
            else:
                score -= 8  # Declining market

        # Rental yield comparison
        if rental_yield is not None:
            benchmark_yield = benchmark["avg_yield"]
            if rental_yield > benchmark_yield * 1.3:
                score += 10  # High yield
            elif rental_yield > benchmark_yield:
                score += 5
            elif rental_yield < benchmark_yield * 0.7:
                score -= 5  # Low yield

        # Proximity to transit (bonus for accessibility)
        if dist_to_train is not None:
            if dist_to_train < 0.5:
                score += 8  # Walking distance
            elif dist_to_train < 1.0:
                score += 5
            elif dist_to_train < 2.0:
                score += 2
            elif dist_to_train > 5.0:
                score -= 3

        # Liquidity (more transactions = more liquid market)
        if transaction_count > 50:
            score += 5
        elif transaction_count > 20:
            score += 3
        elif transaction_count < 5:
            score -= 5  # Low liquidity risk

        # Normalize to 1-100 range
        return max(1, min(100, round(score)))

    def calculate_risk_score(
        self,
        price_std_dev: float,
        avg_price_sqm: float,
        transaction_count: int,
        price_change_1y: Optional[float]
    ) -> float:
        """
        Calculate risk score (1-100).

        Higher score = higher risk.

        Args:
            price_std_dev: Price standard deviation
            avg_price_sqm: Average price per sqm
            transaction_count: Number of transactions
            price_change_1y: 1-year price change

        Returns:
            Risk score between 1-100
        """
        risk = 50.0  # Start at medium risk

        # Price volatility (coefficient of variation)
        if avg_price_sqm and price_std_dev:
            cv = price_std_dev / avg_price_sqm
            if cv > 0.3:
                risk += 15  # High volatility
            elif cv > 0.2:
                risk += 8
            elif cv < 0.1:
                risk -= 10  # Low volatility

        # Market liquidity
        if transaction_count < 5:
            risk += 15  # Very illiquid
        elif transaction_count < 10:
            risk += 8
        elif transaction_count > 50:
            risk -= 10  # Very liquid

        # Price trend instability
        if price_change_1y is not None:
            if abs(price_change_1y) > 20:
                risk += 10  # Volatile market
            elif abs(price_change_1y) > 10:
                risk += 5

        return max(1, min(100, round(risk)))

    def calculate_opportunity_score(
        self,
        roi_score: float,
        risk_score: float
    ) -> float:
        """
        Calculate opportunity score balancing ROI and risk.

        Args:
            roi_score: ROI score (1-100)
            risk_score: Risk score (1-100)

        Returns:
            Opportunity score (1-100)
        """
        # Weight ROI more than risk (investors accept some risk for returns)
        opportunity = (roi_score * 0.65) + ((100 - risk_score) * 0.35)
        return max(1, min(100, round(opportunity)))

    def calculate_full_metrics(
        self,
        location_id: int,
        city: str,
        neighborhood: Optional[str],
        current_transactions: List[Dict],
        historical_transactions: List[Dict] = None,
        rental_data: List[Dict] = None,
        amenity_distances: Dict[str, float] = None
    ) -> ROIMetrics:
        """
        Calculate full ROI metrics for a location.

        Args:
            location_id: Location identifier
            city: City name
            neighborhood: Neighborhood name
            current_transactions: Recent transaction data (last 6 months)
            historical_transactions: Historical transaction data (6-18 months ago)
            rental_data: Rental listing data
            amenity_distances: Distances to amenities

        Returns:
            ROIMetrics object with all calculated values
        """
        # Extract price per sqm values
        current_prices = [
            t.get("price_per_sqm")
            for t in current_transactions
            if t.get("price_per_sqm")
        ]

        historical_prices = []
        if historical_transactions:
            historical_prices = [
                t.get("price_per_sqm")
                for t in historical_transactions
                if t.get("price_per_sqm")
            ]

        # Calculate price metrics
        price_metrics = self.calculate_price_metrics(current_prices)

        # Calculate price changes
        price_change_1y = self.calculate_price_change(current_prices, historical_prices)

        # Calculate rental yield
        avg_rental = None
        rental_yield = None
        if rental_data:
            rental_prices = [r.get("rental_price") for r in rental_data if r.get("rental_price")]
            if rental_prices:
                avg_rental = statistics.mean(rental_prices)
                avg_property_price = price_metrics.get("median_price_sqm", 0) * 80  # Assume 80 sqm avg
                if avg_property_price > 0:
                    rental_yield = self.calculate_rental_yield(avg_property_price, avg_rental)

        # Get distance to train
        dist_to_train = None
        if amenity_distances:
            dist_to_train = amenity_distances.get("dist_to_train_stations_km")

        # Calculate composite scores
        roi_score = self.calculate_roi_score(
            city=city,
            avg_price_sqm=price_metrics.get("avg_price_sqm", 0),
            price_change_1y=price_change_1y,
            rental_yield=rental_yield,
            dist_to_train=dist_to_train,
            transaction_count=price_metrics.get("transaction_count", 0)
        )

        risk_score = self.calculate_risk_score(
            price_std_dev=price_metrics.get("price_std_dev", 0),
            avg_price_sqm=price_metrics.get("avg_price_sqm", 0),
            transaction_count=price_metrics.get("transaction_count", 0),
            price_change_1y=price_change_1y
        )

        opportunity_score = self.calculate_opportunity_score(roi_score, risk_score)

        return ROIMetrics(
            location_id=location_id,
            city=city,
            neighborhood=neighborhood,
            avg_price_sqm=price_metrics.get("avg_price_sqm", 0),
            median_price_sqm=price_metrics.get("median_price_sqm", 0),
            min_price_sqm=price_metrics.get("min_price_sqm", 0),
            max_price_sqm=price_metrics.get("max_price_sqm", 0),
            price_std_dev=price_metrics.get("price_std_dev", 0),
            transaction_count=price_metrics.get("transaction_count", 0),
            price_change_1m=None,  # Would need monthly data
            price_change_3m=None,  # Would need quarterly data
            price_change_1y=price_change_1y,
            avg_rental_price=avg_rental,
            rental_yield_annual=rental_yield,
            roi_score=roi_score,
            risk_score=risk_score,
            opportunity_score=opportunity_score,
            calc_date=datetime.now()
        )


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    calculator = ROICalculator()

    # Sample transaction data
    sample_transactions = [
        {"price_per_sqm": 48000},
        {"price_per_sqm": 52000},
        {"price_per_sqm": 49500},
        {"price_per_sqm": 51000},
        {"price_per_sqm": 47000},
    ]

    historical_transactions = [
        {"price_per_sqm": 45000},
        {"price_per_sqm": 46000},
        {"price_per_sqm": 44500},
    ]

    rental_data = [
        {"rental_price": 6500},
        {"rental_price": 7000},
        {"rental_price": 6800},
    ]

    amenity_distances = {
        "dist_to_train_stations_km": 0.8,
        "dist_to_schools_km": 0.3,
        "dist_to_parks_km": 0.2
    }

    print("=== ROI Calculation Example ===\n")

    metrics = calculator.calculate_full_metrics(
        location_id=1,
        city="תל אביב יפו",
        neighborhood="פלורנטין",
        current_transactions=sample_transactions,
        historical_transactions=historical_transactions,
        rental_data=rental_data,
        amenity_distances=amenity_distances
    )

    print(f"Location: {metrics.city}, {metrics.neighborhood}")
    print(f"Avg Price/sqm: ₪{metrics.avg_price_sqm:,.0f}")
    print(f"Price Change (1Y): {metrics.price_change_1y}%")
    print(f"Rental Yield: {metrics.rental_yield_annual}%")
    print(f"\nScores:")
    print(f"  ROI Score: {metrics.roi_score}/100")
    print(f"  Risk Score: {metrics.risk_score}/100")
    print(f"  Opportunity Score: {metrics.opportunity_score}/100")
