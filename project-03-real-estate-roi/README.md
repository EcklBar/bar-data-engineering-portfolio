# 🏠 הנדל"ניסט החכם | Real Estate ROI Predictor

A comprehensive data engineering pipeline for identifying real estate investment opportunities in Israel.

![Python](https://img.shields.io/badge/Python-3.10+-blue.svg)
![Airflow](https://img.shields.io/badge/Airflow-2.9-green.svg)
![Kafka](https://img.shields.io/badge/Kafka-7.5-orange.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-blue.svg)

## 📋 Project Overview

This project implements a **Multi-Purpose Data Pipeline System** that:
- Acquires real estate data from **Gov.il API** (batch) and simulated listings (streaming)
- Processes and enriches data with **geographic information** from OpenStreetMap
- Calculates **ROI metrics** and investment scores
- Visualizes opportunities through an **interactive dashboard**

### Business Value
Investors can use this system to identify high-ROI real estate opportunities based on:
- Historical transaction prices
- Price appreciation trends
- Rental yield calculations
- Proximity to amenities (train stations, schools, parks)

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DATA SOURCES                                       │
├─────────────────────┬─────────────────────┬─────────────────────────────────┤
│     Gov.il API      │    Kafka Stream     │      OpenStreetMap API          │
│   (Batch - Daily)   │  (Real-time)        │        (Enrichment)             │
└──────────┬──────────┴──────────┬──────────┴───────────────┬─────────────────┘
           │                     │                          │
           ▼                     ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         MINIO (S3-Compatible Storage)                        │
│              Partitioned by: year/month/day                                  │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                    TRANSFORM LAYER (PySpark / Python)                        │
│   • Address Normalization (Hebrew)  • Geocoding  • Feature Engineering      │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                  DATA WAREHOUSE (PostgreSQL + PostGIS)                       │
│         Star Schema: dim_locations, dim_properties, fact_transactions       │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      ORCHESTRATION (Apache Airflow)                          │
│   DAG 1: batch_etl_pipeline (daily)                                         │
│   DAG 2: streaming_monitor (every 15 min)                                   │
│   DAG 3: analytics_refresh (daily)                                          │
└─────────────────────────────────────────────────────────────────────────────┘
           │
           ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PRESENTATION (Streamlit Dashboard)                      │
│   Interactive maps, ROI charts, investment alerts                           │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ Technologies

| Category | Technology | Purpose |
|----------|------------|---------|
| **Orchestration** | Apache Airflow | DAG scheduling, workflow management |
| **Streaming** | Apache Kafka | Real-time data ingestion |
| **Storage** | Minio (S3) | Partitioned raw/processed data |
| **Database** | PostgreSQL + PostGIS | Data warehouse with geospatial |
| **Processing** | PySpark / Python | Data transformations |
| **Dashboard** | Streamlit + Folium | Interactive visualization |
| **Monitoring** | ELK Stack | Logging and monitoring (bonus) |
| **Containerization** | Docker Compose | Full stack deployment |

---

## 📁 Project Structure

```
project-03-real-estate-roi/
├── dags/                           # Airflow DAGs
│   ├── batch_etl_dag.py           # Daily batch ETL
│   ├── streaming_monitor_dag.py   # Kafka monitoring
│   └── analytics_dag.py           # ROI calculation
├── src/
│   ├── extract/
│   │   ├── gov_api.py             # Gov.il API client
│   │   ├── kafka_producer.py      # Kafka producer
│   │   ├── kafka_consumer.py      # Kafka consumer
│   │   └── osm_api.py             # OpenStreetMap API
│   ├── transform/
│   │   ├── address_normalizer.py  # Hebrew address normalization
│   │   └── feature_engineering.py # ROI calculations
│   └── load/
│       ├── minio_client.py        # S3 storage client
│       └── postgres_loader.py     # Data warehouse loader
├── dashboard/
│   ├── app.py                     # Streamlit main app
│   └── components/
│       ├── map_view.py            # Folium maps
│       └── charts.py              # Plotly charts
├── include/
│   ├── sql/create_tables.sql      # Database schema
│   ├── soda/                      # Data quality checks
│   └── logstash/                  # ELK configuration
├── docker/                         # Docker initialization scripts
├── docker-compose.yaml            # Full stack definition
├── requirements.txt               # Python dependencies
└── README.md
```

---

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.10+
- 8GB+ RAM recommended

### 1. Clone and Setup

```bash
# Clone repository
cd project-03-real-estate-roi

# Create environment file
cp .env.example .env

# Edit .env with your passwords
nano .env
```

### 2. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Wait for services to be healthy (2-3 minutes)
docker-compose ps
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | http://localhost:8080 | admin / admin |
| **Minio Console** | http://localhost:9001 | minioadmin / minioadmin |
| **Spark UI** | http://localhost:8081 | - |
| **Kibana** (if enabled) | http://localhost:5601 | - |

### 4. Run the Pipeline

```bash
# Trigger the batch ETL DAG from Airflow UI
# Or run manually:
docker exec airflow-scheduler airflow dags trigger batch_etl_pipeline
```

### 5. Launch Dashboard

```bash
# Install dashboard dependencies
pip install streamlit plotly folium

# Run Streamlit dashboard
cd dashboard
streamlit run app.py
```

---

## 📊 Data Pipeline Details

### Batch ETL Pipeline (Daily at 06:00)

```
extract_gov_data → store_raw_to_minio → transform_addresses → load_to_warehouse → quality_checks → refresh_analytics
```

1. **Extract**: Fetch transactions from Gov.il API
2. **Store**: Save raw JSON to Minio with date partitioning
3. **Transform**: Normalize Hebrew addresses
4. **Load**: Insert/upsert to PostgreSQL
5. **Quality**: Run SODA data quality checks
6. **Analytics**: Refresh ROI metrics

### Streaming Pipeline (Every 15 min)

```
check_kafka_health → check_consumer_lag → process_messages → update_metrics → generate_alerts
```

### Analytics Pipeline (Daily at 08:00)

```
fetch_amenities → fetch_transactions → calculate_roi → save_metrics → generate_alerts
```

---

## 📐 Data Model

### Star Schema

**Dimension Tables:**
- `dim_locations` - Cities, neighborhoods, coordinates, distances to amenities
- `dim_properties` - Property types, rooms, size, year built

**Fact Tables:**
- `fact_transactions` - Historical transaction data from Gov.il
- `fact_listings` - Current listings (streaming)

**Analytics:**
- `agg_roi_metrics` - Calculated ROI scores, price trends, yields

---

## 🎯 Key Features

### Hebrew Address Normalization
Handles inconsistent Israeli address formats:
- `"ת"א"` → `"תל אביב יפו"`
- `"רח' הרצל"` → `"הרצל"`

### ROI Score Calculation (1-100)
Composite score based on:
- Price relative to market benchmark
- Price appreciation trend
- Rental yield
- Proximity to transit
- Market liquidity

### Investment Alerts
Automatic alerts for:
- High ROI opportunities (score > 75)
- Significant price drops
- Emerging neighborhoods

---

## 🧪 Testing

```bash
# Run unit tests
pytest tests/ -v

# Run with coverage
pytest tests/ --cov=src --cov-report=html
```

---

## 📈 Sample Dashboard Screenshots

The dashboard provides:
- 🗺️ **Interactive Map** - Property locations colored by ROI score
- 📊 **ROI Comparison** - Bar charts by city/neighborhood
- 📈 **Price Trends** - Historical price analysis
- 🏆 **Top Opportunities** - Ranked investment opportunities

---

## 🔧 Configuration

### Environment Variables

```bash
# PostgreSQL
POSTGRES_DB=real_estate_dw
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Minio
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=your_password

# Airflow
AIRFLOW_FERNET_KEY=your_fernet_key
```

---

## 📝 License

This project is part of the **Naya College Cloud Big Data Engineer** final project.

---

## 👨‍💻 Author

Data Engineering Portfolio Project

---

## 🙏 Acknowledgments

- **Data Source**: [data.gov.il](https://data.gov.il/dataset/nadlan) - Israel Open Data Portal
- **Map Data**: [OpenStreetMap](https://www.openstreetmap.org/)
- **Course**: Naya College - Cloud Big Data Engineer Program
