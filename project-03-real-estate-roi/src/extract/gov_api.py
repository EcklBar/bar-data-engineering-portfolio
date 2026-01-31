"""
Gov.il Real Estate Transactions API Client
מודול לשליפת נתוני עסקאות נדל"ן מ-data.gov.il

Data source: https://data.gov.il/dataset/nadlan
"""

import logging
import requests
from datetime import datetime, timedelta
from typing import Optional
import time

logger = logging.getLogger(__name__)

# Gov.il CKAN API endpoints
GOV_IL_BASE_URL = "https://data.gov.il/api/3/action"
NADLAN_RESOURCE_ID = "5c995652-79a8-4e85-bb67-e648acfea6cc"  # Real estate transactions


class GovIlApiClient:
    """Client for fetching real estate data from Gov.il Open Data Portal."""

    def __init__(self, base_url: str = GOV_IL_BASE_URL):
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "RealEstateROI-DataPipeline/1.0",
            "Accept": "application/json"
        })

    def fetch_transactions(
        self,
        resource_id: str = NADLAN_RESOURCE_ID,
        limit: int = 1000,
        offset: int = 0,
        filters: Optional[dict] = None,
        city: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None
    ) -> dict:
        """
        Fetch real estate transactions from Gov.il API.

        Args:
            resource_id: The CKAN resource ID for the dataset
            limit: Maximum number of records to fetch (max 32000)
            offset: Number of records to skip
            filters: Dictionary of field filters
            city: Filter by city name (Hebrew)
            from_date: Filter transactions from this date (YYYY-MM-DD)
            to_date: Filter transactions until this date (YYYY-MM-DD)

        Returns:
            Dictionary containing the API response with records
        """
        endpoint = f"{self.base_url}/datastore_search"

        params = {
            "resource_id": resource_id,
            "limit": min(limit, 32000),  # API max is 32000
            "offset": offset
        }

        # Build filters
        filter_dict = filters or {}

        if city:
            filter_dict["SETL_NAME"] = city  # שם יישוב

        if filter_dict:
            import json
            params["filters"] = json.dumps(filter_dict)

        # Add date range query if specified
        if from_date or to_date:
            q_parts = []
            if from_date:
                q_parts.append(f"DEALDATETIME:>={from_date}")
            if to_date:
                q_parts.append(f"DEALDATETIME:<={to_date}")
            params["q"] = " AND ".join(q_parts)

        try:
            logger.info(f"Fetching transactions: limit={limit}, offset={offset}")
            response = self.session.get(endpoint, params=params, timeout=60)
            response.raise_for_status()

            data = response.json()

            if not data.get("success"):
                error_msg = data.get("error", {}).get("message", "Unknown error")
                raise Exception(f"API returned error: {error_msg}")

            result = data.get("result", {})
            records = result.get("records", [])
            total = result.get("total", 0)

            logger.info(f"Fetched {len(records)} records (total available: {total})")
            return {
                "records": records,
                "total": total,
                "offset": offset,
                "limit": limit
            }

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def fetch_all_transactions(
        self,
        city: Optional[str] = None,
        from_date: Optional[str] = None,
        to_date: Optional[str] = None,
        batch_size: int = 5000,
        max_records: Optional[int] = None,
        delay_between_requests: float = 0.5
    ) -> list:
        """
        Fetch all transactions with pagination.

        Args:
            city: Filter by city name
            from_date: Start date (YYYY-MM-DD)
            to_date: End date (YYYY-MM-DD)
            batch_size: Records per request
            max_records: Maximum total records to fetch (None = all)
            delay_between_requests: Seconds to wait between API calls

        Returns:
            List of all transaction records
        """
        all_records = []
        offset = 0

        while True:
            result = self.fetch_transactions(
                limit=batch_size,
                offset=offset,
                city=city,
                from_date=from_date,
                to_date=to_date
            )

            records = result["records"]
            all_records.extend(records)

            logger.info(f"Progress: {len(all_records)}/{result['total']} records")

            # Check if we've fetched all records
            if len(records) < batch_size:
                break

            # Check if we've reached the max
            if max_records and len(all_records) >= max_records:
                all_records = all_records[:max_records]
                break

            offset += batch_size

            # Rate limiting
            time.sleep(delay_between_requests)

        logger.info(f"Total records fetched: {len(all_records)}")
        return all_records

    def get_recent_transactions(self, days: int = 30, city: Optional[str] = None) -> list:
        """
        Fetch transactions from the last N days.

        Args:
            days: Number of days to look back
            city: Optional city filter

        Returns:
            List of recent transaction records
        """
        to_date = datetime.now().strftime("%Y-%m-%d")
        from_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        return self.fetch_all_transactions(
            city=city,
            from_date=from_date,
            to_date=to_date
        )


def parse_transaction_record(record: dict) -> dict:
    """
    Parse and normalize a raw transaction record from Gov.il.

    Field mapping (Hebrew to English):
    - DEALAMOUNT: מחיר העסקה
    - DEALDATETIME: תאריך העסקה
    - DEALNATUREDESCRIPTION: אופי העסקה
    - NEWPROJECTTEXT: פרויקט חדש
    - ASSETROOMNUM: מספר חדרים
    - FLOORNO: קומה
    - BUILDINGYEAR: שנת בנייה
    - BUILDINGFLOORS: מספר קומות בבניין
    - GUSH: גוש
    - HELKA: חלקה
    - TATHELKA: תת חלקה
    - SETL_NAME: שם יישוב
    - STREET: רחוב
    - HOUSENO: מספר בית
    - OBJECTIVEDISC: סוג הנכס
    - DEALAREASMETER: שטח במ"ר

    Returns:
        Normalized dictionary with consistent field names
    """
    def safe_float(value, default=None):
        try:
            return float(value) if value else default
        except (ValueError, TypeError):
            return default

    def safe_int(value, default=None):
        try:
            return int(float(value)) if value else default
        except (ValueError, TypeError):
            return default

    # Parse date
    deal_date = None
    if record.get("DEALDATETIME"):
        try:
            deal_date = datetime.strptime(
                str(record["DEALDATETIME"])[:10], "%Y-%m-%d"
            ).date()
        except ValueError:
            pass

    price = safe_float(record.get("DEALAMOUNT"))
    size_sqm = safe_float(record.get("DEALAREASMETER"))

    # Calculate price per sqm
    price_per_sqm = None
    if price and size_sqm and size_sqm > 0:
        price_per_sqm = round(price / size_sqm, 2)

    return {
        # Location
        "city": record.get("SETL_NAME", "").strip(),
        "street": record.get("STREET", "").strip(),
        "house_number": str(record.get("HOUSENO", "")).strip(),
        "gush": record.get("GUSH"),
        "helka": record.get("HELKA"),

        # Property details
        "property_type": record.get("OBJECTIVEDISC", "").strip(),
        "rooms": safe_float(record.get("ASSETROOMNUM")),
        "floor": safe_int(record.get("FLOORNO")),
        "total_floors": safe_int(record.get("BUILDINGFLOORS")),
        "size_sqm": size_sqm,
        "year_built": safe_int(record.get("BUILDINGYEAR")),

        # Transaction details
        "price": price,
        "price_per_sqm": price_per_sqm,
        "transaction_date": deal_date,
        "deal_nature": record.get("DEALNATUREDESCRIPTION", "").strip(),
        "is_new_project": record.get("NEWPROJECTTEXT") == "כן",

        # Metadata
        "source_id": f"{record.get('GUSH')}_{record.get('HELKA')}_{record.get('DEALDATETIME')}",
        "raw_data": record
    }


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    client = GovIlApiClient()

    # Fetch sample data
    print("Fetching sample transactions...")
    result = client.fetch_transactions(limit=5)

    print(f"\nTotal available records: {result['total']}")
    print(f"\nSample records:")

    for i, record in enumerate(result["records"][:3], 1):
        parsed = parse_transaction_record(record)
        print(f"\n--- Record {i} ---")
        print(f"City: {parsed['city']}")
        print(f"Street: {parsed['street']} {parsed['house_number']}")
        print(f"Price: {parsed['price']:,.0f} ₪" if parsed['price'] else "Price: N/A")
        print(f"Size: {parsed['size_sqm']} sqm" if parsed['size_sqm'] else "Size: N/A")
        print(f"Price/sqm: {parsed['price_per_sqm']:,.0f} ₪" if parsed['price_per_sqm'] else "")
        print(f"Date: {parsed['transaction_date']}")
