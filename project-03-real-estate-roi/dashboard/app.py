"""
Real Estate ROI Dashboard
דשבורד לניתוח הזדמנויות השקעה בנדל"ן

Streamlit dashboard for visualizing real estate investment opportunities.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from components.map_view import create_map, create_heatmap
from components.charts import (
    create_price_trend_chart,
    create_roi_comparison_chart,
    create_city_breakdown_chart
)

# Page config
st.set_page_config(
    page_title="הנדל״ניסט החכם | Real Estate ROI",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for RTL support
st.markdown("""
<style>
    .rtl {
        direction: rtl;
        text-align: right;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 20px;
        border-radius: 10px;
        text-align: center;
    }
    .stMetric {
        background-color: #ffffff;
        padding: 15px;
        border-radius: 8px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }
</style>
""", unsafe_allow_html=True)


# ============================================
# Data Loading Functions
# ============================================

@st.cache_data(ttl=3600)
def load_roi_metrics():
    """Load ROI metrics from database."""
    try:
        from load.postgres_loader import PostgresLoader
        loader = PostgresLoader()

        query = """
        SELECT
            city,
            neighborhood,
            avg_price_sqm,
            median_price_sqm,
            price_change_1y,
            avg_rental_yield,
            roi_score,
            transaction_count,
            calc_date
        FROM analytics.agg_roi_metrics
        WHERE calc_date = (SELECT MAX(calc_date) FROM analytics.agg_roi_metrics)
        ORDER BY roi_score DESC
        """
        return loader.fetch_query(query)

    except Exception as e:
        st.warning(f"Could not load data from database: {e}")
        return get_sample_data()


@st.cache_data(ttl=3600)
def load_price_history():
    """Load price history for trend analysis."""
    try:
        from load.postgres_loader import PostgresLoader
        loader = PostgresLoader()

        query = """
        SELECT
            city,
            DATE_TRUNC('month', transaction_date) as month,
            AVG(price_per_sqm) as avg_price_sqm,
            COUNT(*) as transaction_count
        FROM core.fact_transactions t
        JOIN core.dim_locations l ON t.location_id = l.location_id
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '2 years'
        GROUP BY city, DATE_TRUNC('month', transaction_date)
        ORDER BY city, month
        """
        return loader.fetch_query(query)

    except Exception as e:
        return pd.DataFrame()


def get_sample_data():
    """Return sample data for demo purposes."""
    return pd.DataFrame({
        'city': ['תל אביב יפו', 'תל אביב יפו', 'ירושלים', 'ירושלים', 'חיפה',
                 'רמת גן', 'הרצליה', 'ראשון לציון', 'פתח תקווה', 'נתניה'],
        'neighborhood': ['פלורנטין', 'רמת אביב', 'רחביה', 'בקעה', 'כרמל',
                        'מרכז', 'פיתוח', 'נחלת יהודה', 'מרכז', 'מרכז'],
        'avg_price_sqm': [52000, 65000, 48000, 42000, 28000,
                         45000, 58000, 38000, 35000, 32000],
        'median_price_sqm': [50000, 63000, 46000, 40000, 27000,
                            43000, 56000, 36000, 33000, 30000],
        'price_change_1y': [8.5, 6.2, 5.8, 7.1, 4.2, 6.8, 9.2, 5.5, 4.8, 6.1],
        'avg_rental_yield': [2.8, 2.4, 3.2, 3.5, 4.2, 3.0, 2.5, 3.4, 3.6, 3.8],
        'roi_score': [78, 72, 75, 82, 68, 74, 71, 76, 73, 77],
        'transaction_count': [145, 98, 112, 87, 156, 134, 76, 167, 189, 145],
        'calc_date': [datetime.now().date()] * 10
    })


# ============================================
# Main Dashboard
# ============================================

def main():
    # Header
    st.title("🏠 הנדל״ניסט החכם")
    st.subheader("Real Estate ROI Predictor")

    # Load data
    df = load_roi_metrics()

    if df.empty:
        st.error("No data available. Please run the ETL pipeline first.")
        return

    # Sidebar filters
    st.sidebar.header("🔍 סינון | Filters")

    cities = ['All'] + sorted(df['city'].unique().tolist())
    selected_city = st.sidebar.selectbox("עיר | City", cities)

    min_roi = st.sidebar.slider(
        "Minimum ROI Score",
        min_value=0,
        max_value=100,
        value=50
    )

    # Filter data
    filtered_df = df.copy()
    if selected_city != 'All':
        filtered_df = filtered_df[filtered_df['city'] == selected_city]
    filtered_df = filtered_df[filtered_df['roi_score'] >= min_roi]

    # Key Metrics Row
    st.markdown("---")
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="📊 Average ROI Score",
            value=f"{filtered_df['roi_score'].mean():.1f}",
            delta=None
        )

    with col2:
        st.metric(
            label="💰 Avg Price/sqm",
            value=f"₪{filtered_df['avg_price_sqm'].mean():,.0f}",
            delta=None
        )

    with col3:
        st.metric(
            label="📈 Avg Price Change (1Y)",
            value=f"{filtered_df['price_change_1y'].mean():.1f}%",
            delta=None
        )

    with col4:
        st.metric(
            label="🏘️ Locations Analyzed",
            value=f"{len(filtered_df)}",
            delta=None
        )

    st.markdown("---")

    # Main content tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🗺️ Map View",
        "📊 ROI Analysis",
        "📈 Price Trends",
        "🏆 Top Opportunities"
    ])

    # Tab 1: Map View
    with tab1:
        st.subheader("Investment Opportunity Map")

        # Create map
        try:
            map_html = create_map(filtered_df)
            st.components.v1.html(map_html, height=500)
        except Exception as e:
            st.info("Map visualization requires location coordinates in the database.")
            st.write("Sample data locations will be shown when database is connected.")

    # Tab 2: ROI Analysis
    with tab2:
        col1, col2 = st.columns(2)

        with col1:
            st.subheader("ROI Score by City")
            fig = create_roi_comparison_chart(filtered_df)
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.subheader("Price vs ROI Score")
            fig = px.scatter(
                filtered_df,
                x='avg_price_sqm',
                y='roi_score',
                color='city',
                size='transaction_count',
                hover_data=['neighborhood', 'price_change_1y'],
                labels={
                    'avg_price_sqm': 'Price per sqm (₪)',
                    'roi_score': 'ROI Score',
                    'city': 'City'
                }
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)

        # City breakdown
        st.subheader("City Breakdown")
        fig = create_city_breakdown_chart(filtered_df)
        st.plotly_chart(fig, use_container_width=True)

    # Tab 3: Price Trends
    with tab3:
        st.subheader("Price Trends Over Time")

        price_history = load_price_history()

        if not price_history.empty:
            fig = create_price_trend_chart(price_history)
            st.plotly_chart(fig, use_container_width=True)
        else:
            # Show sample trend
            st.info("Historical price data will be displayed when available.")

            # Sample trend data
            months = pd.date_range(end=datetime.now(), periods=24, freq='M')
            sample_trend = pd.DataFrame({
                'month': list(months) * 3,
                'city': ['תל אביב יפו'] * 24 + ['ירושלים'] * 24 + ['חיפה'] * 24,
                'avg_price_sqm': (
                    [45000 + i * 400 for i in range(24)] +
                    [38000 + i * 350 for i in range(24)] +
                    [22000 + i * 200 for i in range(24)]
                )
            })

            fig = px.line(
                sample_trend,
                x='month',
                y='avg_price_sqm',
                color='city',
                title='Sample Price Trend (Demo Data)'
            )
            st.plotly_chart(fig, use_container_width=True)

    # Tab 4: Top Opportunities
    with tab4:
        st.subheader("🏆 Top Investment Opportunities")

        top_opportunities = filtered_df.nlargest(10, 'roi_score')

        # Display as cards
        for idx, row in top_opportunities.iterrows():
            with st.container():
                col1, col2, col3, col4, col5 = st.columns([2, 1, 1, 1, 1])

                with col1:
                    st.markdown(f"**{row['city']}, {row['neighborhood']}**")

                with col2:
                    st.metric("ROI Score", f"{row['roi_score']:.0f}")

                with col3:
                    st.metric("Price/sqm", f"₪{row['avg_price_sqm']:,.0f}")

                with col4:
                    delta_color = "normal" if row['price_change_1y'] > 0 else "inverse"
                    st.metric("1Y Change", f"{row['price_change_1y']:.1f}%")

                with col5:
                    st.metric("Yield", f"{row['avg_rental_yield']:.1f}%")

                st.markdown("---")

        # Data table
        st.subheader("📋 Full Data Table")
        st.dataframe(
            filtered_df.sort_values('roi_score', ascending=False),
            use_container_width=True,
            hide_index=True
        )

    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>🏠 הנדל״ניסט החכם | Real Estate ROI Predictor</p>
            <p>Data Engineering Final Project - Naya College</p>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
