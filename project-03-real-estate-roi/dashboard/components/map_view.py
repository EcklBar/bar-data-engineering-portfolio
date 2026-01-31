"""
Map Visualization Components
רכיבי ויזואליזציה של מפות

Folium-based map components for the dashboard.
"""

import folium
from folium.plugins import HeatMap, MarkerCluster
import pandas as pd
from typing import Optional


# Israel center coordinates
ISRAEL_CENTER = [31.5, 34.75]

# City coordinates for fallback
CITY_COORDS = {
    "תל אביב יפו": [32.0853, 34.7818],
    "ירושלים": [31.7683, 35.2137],
    "חיפה": [32.7940, 34.9896],
    "באר שבע": [31.2520, 34.7915],
    "ראשון לציון": [31.9730, 34.7925],
    "פתח תקווה": [32.0841, 34.8878],
    "נתניה": [32.3215, 34.8532],
    "חולון": [32.0158, 34.7795],
    "רמת גן": [32.0680, 34.8241],
    "אשדוד": [31.8014, 34.6435],
    "הרצליה": [32.1656, 34.8464],
    "כפר סבא": [32.1780, 34.9065],
    "רעננה": [32.1836, 34.8706],
    "מודיעין מכבים רעות": [31.8977, 35.0107],
}


def get_roi_color(roi_score: float) -> str:
    """
    Get color based on ROI score.

    Args:
        roi_score: ROI score (1-100)

    Returns:
        Color hex code
    """
    if roi_score >= 80:
        return '#22c55e'  # Green - Excellent
    elif roi_score >= 70:
        return '#84cc16'  # Light green - Good
    elif roi_score >= 60:
        return '#eab308'  # Yellow - Average
    elif roi_score >= 50:
        return '#f97316'  # Orange - Below average
    else:
        return '#ef4444'  # Red - Poor


def create_map(df: pd.DataFrame, zoom_start: int = 8) -> str:
    """
    Create an interactive map with ROI markers.

    Args:
        df: DataFrame with city, neighborhood, roi_score, etc.
        zoom_start: Initial zoom level

    Returns:
        HTML string of the map
    """
    # Create base map
    m = folium.Map(
        location=ISRAEL_CENTER,
        zoom_start=zoom_start,
        tiles='cartodbpositron'
    )

    # Create marker cluster
    marker_cluster = MarkerCluster().add_to(m)

    # Add markers for each location
    for _, row in df.iterrows():
        city = row.get('city', '')

        # Get coordinates (from data or fallback to city center)
        lat = row.get('lat') or (CITY_COORDS.get(city, ISRAEL_CENTER)[0])
        lon = row.get('lng') or row.get('lon') or (CITY_COORDS.get(city, ISRAEL_CENTER)[1])

        # Add some jitter for locations in same city
        import random
        lat += random.uniform(-0.01, 0.01)
        lon += random.uniform(-0.01, 0.01)

        roi_score = row.get('roi_score', 50)
        color = get_roi_color(roi_score)

        # Create popup content
        popup_html = f"""
        <div style="font-family: Arial; direction: rtl; text-align: right;">
            <h4 style="margin: 0; color: {color};">{city}</h4>
            <p style="margin: 5px 0;"><b>{row.get('neighborhood', '')}</b></p>
            <hr style="margin: 5px 0;">
            <p><b>ROI Score:</b> {roi_score:.0f}/100</p>
            <p><b>Price/sqm:</b> ₪{row.get('avg_price_sqm', 0):,.0f}</p>
            <p><b>1Y Change:</b> {row.get('price_change_1y', 0):.1f}%</p>
            <p><b>Yield:</b> {row.get('avg_rental_yield', 0):.1f}%</p>
            <p><b>Transactions:</b> {row.get('transaction_count', 0)}</p>
        </div>
        """

        # Add marker
        folium.CircleMarker(
            location=[lat, lon],
            radius=8 + (roi_score / 10),  # Size based on ROI
            color=color,
            fill=True,
            fill_color=color,
            fill_opacity=0.7,
            popup=folium.Popup(popup_html, max_width=250),
            tooltip=f"{city}: ROI {roi_score:.0f}"
        ).add_to(marker_cluster)

    # Add legend
    legend_html = """
    <div style="
        position: fixed;
        bottom: 50px;
        left: 50px;
        z-index: 1000;
        background-color: white;
        padding: 10px;
        border-radius: 5px;
        box-shadow: 0 2px 5px rgba(0,0,0,0.2);
        font-family: Arial;
    ">
        <h4 style="margin: 0 0 10px 0;">ROI Score</h4>
        <div><span style="color: #22c55e;">●</span> 80+ Excellent</div>
        <div><span style="color: #84cc16;">●</span> 70-79 Good</div>
        <div><span style="color: #eab308;">●</span> 60-69 Average</div>
        <div><span style="color: #f97316;">●</span> 50-59 Below Avg</div>
        <div><span style="color: #ef4444;">●</span> <50 Poor</div>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))

    return m._repr_html_()


def create_heatmap(df: pd.DataFrame) -> str:
    """
    Create a heatmap based on transaction density.

    Args:
        df: DataFrame with location data

    Returns:
        HTML string of the heatmap
    """
    # Create base map
    m = folium.Map(
        location=ISRAEL_CENTER,
        zoom_start=8,
        tiles='cartodbpositron'
    )

    # Prepare heat data
    heat_data = []

    for _, row in df.iterrows():
        city = row.get('city', '')
        lat = row.get('lat') or (CITY_COORDS.get(city, ISRAEL_CENTER)[0])
        lon = row.get('lng') or row.get('lon') or (CITY_COORDS.get(city, ISRAEL_CENTER)[1])

        # Weight by transaction count
        weight = row.get('transaction_count', 1) / 100

        heat_data.append([lat, lon, weight])

    # Add heatmap
    HeatMap(
        heat_data,
        min_opacity=0.3,
        radius=25,
        blur=15,
        gradient={0.4: 'blue', 0.65: 'lime', 1: 'red'}
    ).add_to(m)

    return m._repr_html_()


def create_city_map(city: str, df: pd.DataFrame) -> str:
    """
    Create a detailed map for a specific city.

    Args:
        city: City name
        df: DataFrame filtered for the city

    Returns:
        HTML string of the map
    """
    # Get city center
    center = CITY_COORDS.get(city, ISRAEL_CENTER)

    # Create map
    m = folium.Map(
        location=center,
        zoom_start=13,
        tiles='cartodbpositron'
    )

    # Add markers for neighborhoods
    for _, row in df[df['city'] == city].iterrows():
        lat = row.get('lat') or center[0] + (hash(row.get('neighborhood', '')) % 100) / 5000
        lon = row.get('lng') or center[1] + (hash(row.get('neighborhood', '')) % 100) / 5000

        roi_score = row.get('roi_score', 50)
        color = get_roi_color(roi_score)

        folium.Marker(
            location=[lat, lon],
            popup=f"{row.get('neighborhood', 'N/A')}<br>ROI: {roi_score:.0f}",
            icon=folium.Icon(color='green' if roi_score >= 70 else 'orange' if roi_score >= 50 else 'red')
        ).add_to(m)

    return m._repr_html_()
