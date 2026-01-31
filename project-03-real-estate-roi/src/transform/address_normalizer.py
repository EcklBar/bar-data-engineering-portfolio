"""
Hebrew Address Normalizer
מודול לנרמול כתובות בעברית

Handles inconsistent address formats commonly found in Israeli real estate data.
"""

import re
import logging
from typing import Optional, Tuple
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class NormalizedAddress:
    """Normalized address structure."""
    city: str
    neighborhood: Optional[str]
    street: Optional[str]
    house_number: Optional[str]
    entrance: Optional[str]
    apartment: Optional[str]
    original: str

    def to_dict(self) -> dict:
        return {
            "city": self.city,
            "neighborhood": self.neighborhood,
            "street": self.street,
            "house_number": self.house_number,
            "entrance": self.entrance,
            "apartment": self.apartment,
            "original": self.original
        }

    def full_address(self) -> str:
        """Return formatted full address."""
        parts = []
        if self.street:
            street_part = self.street
            if self.house_number:
                street_part += f" {self.house_number}"
            parts.append(street_part)
        if self.city:
            parts.append(self.city)
        return ", ".join(parts)


# ============================================
# City Name Aliases (Common variations)
# ============================================
CITY_ALIASES = {
    # Tel Aviv variations
    'ת"א': 'תל אביב יפו',
    'תל אביב': 'תל אביב יפו',
    'תל-אביב': 'תל אביב יפו',
    'תל אביב-יפו': 'תל אביב יפו',
    'ת"א-יפו': 'תל אביב יפו',

    # Jerusalem variations
    'י-ם': 'ירושלים',
    'ירושלם': 'ירושלים',

    # Haifa variations
    'חיפא': 'חיפה',

    # Beer Sheva variations
    'באר שבע': 'באר שבע',
    'ב"ש': 'באר שבע',
    'באר-שבע': 'באר שבע',

    # Rishon LeZion variations
    'ראשל"צ': 'ראשון לציון',
    'ראשון לצ': 'ראשון לציון',
    'ראשון-לציון': 'ראשון לציון',

    # Petah Tikva variations
    'פ"ת': 'פתח תקווה',
    'פתח תקוה': 'פתח תקווה',
    'פתח-תקווה': 'פתח תקווה',

    # Netanya variations
    'נתניא': 'נתניה',

    # Ramat Gan variations
    'ר"ג': 'רמת גן',
    'רמת-גן': 'רמת גן',

    # Bnei Brak variations
    'בנ"ב': 'בני ברק',
    'בני-ברק': 'בני ברק',

    # Holon variations
    'חולון': 'חולון',

    # Bat Yam variations
    'בת-ים': 'בת ים',

    # Ashdod variations
    'אשדוד': 'אשדוד',

    # Herzliya variations
    'הרצליא': 'הרצליה',

    # Kfar Saba variations
    'כפר-סבא': 'כפר סבא',
    'כ"ס': 'כפר סבא',

    # Ra'anana variations
    'רעננא': 'רעננה',

    # Modiin variations
    'מודיעין': 'מודיעין מכבים רעות',
    'מודיעין-מכבים-רעות': 'מודיעין מכבים רעות',
}

# ============================================
# Street Prefixes to Remove/Normalize
# ============================================
STREET_PREFIXES = [
    'רח\'',
    'רחוב',
    'רח',
    'שד\'',
    'שדרות',
    'שד',
    'דרך',
    'כביש',
    'ככר',
    'מעבר',
    'סמטת',
    'סמ\'',
]

# Compile regex for street prefix removal
STREET_PREFIX_PATTERN = re.compile(
    r'^(' + '|'.join(re.escape(p) for p in STREET_PREFIXES) + r')\s*',
    re.UNICODE
)

# ============================================
# House Number Patterns
# ============================================
# Matches: 15, 15א, 15/2, 15-17, 15 א
HOUSE_NUMBER_PATTERN = re.compile(
    r'(\d+)\s*([א-ת])?(?:\s*[-/]\s*(\d+))?',
    re.UNICODE
)


def normalize_city(city: str) -> str:
    """
    Normalize city name to standard format.

    Args:
        city: Raw city name

    Returns:
        Normalized city name
    """
    if not city:
        return ""

    # Clean whitespace
    city = city.strip()

    # Remove extra spaces
    city = re.sub(r'\s+', ' ', city)

    # Check aliases
    city_upper = city
    if city_upper in CITY_ALIASES:
        return CITY_ALIASES[city_upper]

    # Check case-insensitive (for mixed Hebrew/English)
    for alias, normalized in CITY_ALIASES.items():
        if city.lower() == alias.lower():
            return normalized

    return city


def normalize_street(street: str) -> str:
    """
    Normalize street name by removing prefixes.

    Args:
        street: Raw street name

    Returns:
        Normalized street name without prefix
    """
    if not street:
        return ""

    # Clean whitespace
    street = street.strip()

    # Remove prefix
    street = STREET_PREFIX_PATTERN.sub('', street)

    # Remove extra spaces
    street = re.sub(r'\s+', ' ', street)

    return street.strip()


def extract_house_number(address: str) -> Tuple[str, Optional[str]]:
    """
    Extract house number from address string.

    Args:
        address: Address string that may contain house number

    Returns:
        Tuple of (address without number, house number or None)
    """
    # Look for house number at the end
    match = re.search(r'\s+(\d+\s*[א-ת]?(?:\s*[-/]\s*\d+)?)\s*$', address)

    if match:
        house_number = match.group(1).strip()
        address_without = address[:match.start()].strip()
        return address_without, house_number

    return address, None


def normalize_address(
    city: Optional[str] = None,
    street: Optional[str] = None,
    house_number: Optional[str] = None,
    full_address: Optional[str] = None
) -> NormalizedAddress:
    """
    Normalize an Israeli address.

    Can accept either individual components or a full address string.

    Args:
        city: City name
        street: Street name
        house_number: House number
        full_address: Complete address string to parse

    Returns:
        NormalizedAddress object
    """
    original = full_address or f"{street} {house_number}, {city}".strip(", ")

    # If full address provided, try to parse it
    if full_address and not (city or street):
        # Try to split by comma
        parts = [p.strip() for p in full_address.split(',')]

        if len(parts) >= 2:
            # Assume format: "street number, city"
            street_part = parts[0]
            city = parts[-1]

            # Extract house number from street part
            street, house_number = extract_house_number(street_part)
        elif len(parts) == 1:
            # Try to parse single string
            street, house_number = extract_house_number(parts[0])

    # Normalize components
    normalized_city = normalize_city(city or "")
    normalized_street = normalize_street(street or "")

    # Clean house number
    normalized_house = None
    if house_number:
        normalized_house = str(house_number).strip()

    return NormalizedAddress(
        city=normalized_city,
        neighborhood=None,  # Would need external data to determine
        street=normalized_street,
        house_number=normalized_house,
        entrance=None,
        apartment=None,
        original=original
    )


def normalize_address_batch(records: list, city_field: str = "city",
                           street_field: str = "street",
                           house_field: str = "house_number") -> list:
    """
    Normalize addresses in a batch of records.

    Args:
        records: List of dictionaries containing address fields
        city_field: Name of city field in records
        street_field: Name of street field in records
        house_field: Name of house number field in records

    Returns:
        Records with normalized address fields added
    """
    normalized_records = []

    for record in records:
        normalized = normalize_address(
            city=record.get(city_field),
            street=record.get(street_field),
            house_number=record.get(house_field)
        )

        # Add normalized fields
        record["normalized_city"] = normalized.city
        record["normalized_street"] = normalized.street
        record["normalized_house_number"] = normalized.house_number
        record["full_address"] = normalized.full_address()

        normalized_records.append(record)

    return normalized_records


# ============================================
# Testing / Examples
# ============================================
if __name__ == "__main__":
    # Test cases
    test_cases = [
        # City normalization
        ("ת\"א", None, None),
        ("תל אביב", None, None),
        ("י-ם", None, None),
        ("ראשל\"צ", None, None),

        # Street normalization
        ("תל אביב יפו", "רח' הרצל", "15"),
        ("ירושלים", "שדרות בן גוריון", "42"),
        ("חיפה", "רחוב העצמאות", "7א"),

        # Full address parsing
        (None, None, None),  # Will use full_address
    ]

    print("=== Address Normalization Tests ===\n")

    # Test individual components
    for city, street, house in test_cases[:-1]:
        normalized = normalize_address(city=city, street=street, house_number=house)
        print(f"Input: city='{city}', street='{street}', house='{house}'")
        print(f"Output: {normalized.full_address()}")
        print(f"  City: {normalized.city}")
        print(f"  Street: {normalized.street}")
        print(f"  House: {normalized.house_number}")
        print()

    # Test full address parsing
    full_addresses = [
        "רח' הרצל 15, תל אביב",
        "שדרות רוטשילד 50, ת\"א",
        "דרך בן גוריון 100, רמת גן",
        "העצמאות 7, חיפה",
    ]

    print("=== Full Address Parsing ===\n")
    for addr in full_addresses:
        normalized = normalize_address(full_address=addr)
        print(f"Input: '{addr}'")
        print(f"Output: {normalized.full_address()}")
        print(f"  Parsed city: {normalized.city}")
        print(f"  Parsed street: {normalized.street}")
        print(f"  Parsed house: {normalized.house_number}")
        print()
