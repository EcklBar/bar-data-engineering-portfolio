"""
Chart Components for Dashboard
רכיבי גרפים לדשבורד

Plotly-based chart components for data visualization.
"""

import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import pandas as pd


def create_roi_comparison_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create a bar chart comparing ROI scores by city.

    Args:
        df: DataFrame with city and roi_score columns

    Returns:
        Plotly figure
    """
    # Aggregate by city
    city_avg = df.groupby('city').agg({
        'roi_score': 'mean',
        'transaction_count': 'sum'
    }).reset_index()

    city_avg = city_avg.sort_values('roi_score', ascending=True)

    # Create color scale based on ROI
    colors = city_avg['roi_score'].apply(
        lambda x: '#22c55e' if x >= 75 else '#84cc16' if x >= 65 else '#eab308' if x >= 55 else '#f97316'
    )

    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=city_avg['city'],
        x=city_avg['roi_score'],
        orientation='h',
        marker_color=colors,
        text=city_avg['roi_score'].round(1),
        textposition='outside',
        hovertemplate=(
            '<b>%{y}</b><br>'
            'ROI Score: %{x:.1f}<br>'
            '<extra></extra>'
        )
    ))

    fig.update_layout(
        title='Average ROI Score by City',
        xaxis_title='ROI Score',
        yaxis_title='',
        height=400,
        showlegend=False,
        xaxis=dict(range=[0, 100]),
        margin=dict(l=120, r=20, t=40, b=40)
    )

    return fig


def create_price_trend_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create a line chart showing price trends over time.

    Args:
        df: DataFrame with month, city, and avg_price_sqm columns

    Returns:
        Plotly figure
    """
    fig = px.line(
        df,
        x='month',
        y='avg_price_sqm',
        color='city',
        title='Price Trends by City',
        labels={
            'month': 'Date',
            'avg_price_sqm': 'Average Price per sqm (₪)',
            'city': 'City'
        }
    )

    fig.update_layout(
        height=400,
        legend=dict(
            orientation='h',
            yanchor='bottom',
            y=1.02,
            xanchor='right',
            x=1
        ),
        hovermode='x unified'
    )

    fig.update_traces(mode='lines+markers')

    return fig


def create_city_breakdown_chart(df: pd.DataFrame) -> go.Figure:
    """
    Create a multi-metric breakdown chart by city.

    Args:
        df: DataFrame with city metrics

    Returns:
        Plotly figure
    """
    # Aggregate by city
    city_data = df.groupby('city').agg({
        'avg_price_sqm': 'mean',
        'roi_score': 'mean',
        'price_change_1y': 'mean',
        'avg_rental_yield': 'mean',
        'transaction_count': 'sum'
    }).reset_index()

    # Create subplots
    fig = make_subplots(
        rows=1, cols=4,
        subplot_titles=['Avg Price/sqm', 'ROI Score', '1Y Price Change', 'Rental Yield'],
        specs=[[{'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}, {'type': 'bar'}]]
    )

    # Price per sqm
    fig.add_trace(
        go.Bar(
            x=city_data['city'],
            y=city_data['avg_price_sqm'],
            marker_color='#3b82f6',
            name='Price/sqm'
        ),
        row=1, col=1
    )

    # ROI Score
    fig.add_trace(
        go.Bar(
            x=city_data['city'],
            y=city_data['roi_score'],
            marker_color='#22c55e',
            name='ROI Score'
        ),
        row=1, col=2
    )

    # Price Change
    colors = ['#22c55e' if x > 0 else '#ef4444' for x in city_data['price_change_1y']]
    fig.add_trace(
        go.Bar(
            x=city_data['city'],
            y=city_data['price_change_1y'],
            marker_color=colors,
            name='1Y Change %'
        ),
        row=1, col=3
    )

    # Rental Yield
    fig.add_trace(
        go.Bar(
            x=city_data['city'],
            y=city_data['avg_rental_yield'],
            marker_color='#f59e0b',
            name='Yield %'
        ),
        row=1, col=4
    )

    fig.update_layout(
        height=350,
        showlegend=False,
        margin=dict(l=20, r=20, t=40, b=80)
    )

    # Rotate x-axis labels
    fig.update_xaxes(tickangle=45)

    return fig


def create_scatter_matrix(df: pd.DataFrame) -> go.Figure:
    """
    Create a scatter matrix for exploring correlations.

    Args:
        df: DataFrame with numeric columns

    Returns:
        Plotly figure
    """
    numeric_cols = ['avg_price_sqm', 'roi_score', 'price_change_1y', 'avg_rental_yield']

    fig = px.scatter_matrix(
        df,
        dimensions=numeric_cols,
        color='city',
        title='Metric Correlations',
        labels={col: col.replace('_', ' ').title() for col in numeric_cols}
    )

    fig.update_layout(height=700)
    fig.update_traces(diagonal_visible=False)

    return fig


def create_opportunity_gauge(roi_score: float) -> go.Figure:
    """
    Create a gauge chart for ROI score.

    Args:
        roi_score: ROI score value (1-100)

    Returns:
        Plotly figure
    """
    fig = go.Figure(go.Indicator(
        mode="gauge+number+delta",
        value=roi_score,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "ROI Score"},
        delta={'reference': 50},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 50], 'color': "#fee2e2"},
                {'range': [50, 70], 'color': "#fef3c7"},
                {'range': [70, 85], 'color': "#d1fae5"},
                {'range': [85, 100], 'color': "#a7f3d0"}
            ],
            'threshold': {
                'line': {'color': "red", 'width': 4},
                'thickness': 0.75,
                'value': 75
            }
        }
    ))

    fig.update_layout(height=250)

    return fig


def create_treemap(df: pd.DataFrame) -> go.Figure:
    """
    Create a treemap showing transaction distribution.

    Args:
        df: DataFrame with city, neighborhood, and transaction_count

    Returns:
        Plotly figure
    """
    fig = px.treemap(
        df,
        path=['city', 'neighborhood'],
        values='transaction_count',
        color='roi_score',
        color_continuous_scale='RdYlGn',
        title='Transaction Volume by Location'
    )

    fig.update_layout(height=500)

    return fig
