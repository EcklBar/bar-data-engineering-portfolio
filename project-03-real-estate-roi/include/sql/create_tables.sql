-- ============================================
-- Real Estate ROI Data Warehouse Schema
-- הנדל"ניסט החכם - סכמת מחסן נתונים
-- ============================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS analytics;

-- ============================================
-- STAGING SCHEMA - Raw data landing zone
-- ============================================

-- Raw transactions from Gov.il
CREATE TABLE IF NOT EXISTS staging.raw_transactions (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    source VARCHAR(50) DEFAULT 'gov_il',
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw listings from scraping
CREATE TABLE IF NOT EXISTS staging.raw_listings (
    id SERIAL PRIMARY KEY,
    raw_data JSONB NOT NULL,
    source VARCHAR(50),
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- CORE SCHEMA - Dimensional Model (Star Schema)
-- ============================================

-- Dimension: Locations
CREATE TABLE IF NOT EXISTS core.dim_locations (
    location_id SERIAL PRIMARY KEY,
    city VARCHAR(100) NOT NULL,
    neighborhood VARCHAR(100),
    street VARCHAR(200),
    house_number VARCHAR(20),
    lat DECIMAL(10, 7),
    lng DECIMAL(10, 7),
    geom GEOMETRY(Point, 4326),  -- PostGIS geometry (WGS84)
    dist_to_train_km DECIMAL(5, 2),
    dist_to_school_km DECIMAL(5, 2),
    dist_to_park_km DECIMAL(5, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, neighborhood, street, house_number)
);

-- Create spatial index on geometry column
CREATE INDEX IF NOT EXISTS idx_dim_locations_geom ON core.dim_locations USING GIST (geom);
CREATE INDEX IF NOT EXISTS idx_dim_locations_city ON core.dim_locations (city);

-- Dimension: Property Types
CREATE TABLE IF NOT EXISTS core.dim_properties (
    property_id SERIAL PRIMARY KEY,
    property_type VARCHAR(50) NOT NULL,  -- דירה, בית, פנטהאוז, וילה
    rooms DECIMAL(3, 1),
    floor INTEGER,
    total_floors INTEGER,
    size_sqm DECIMAL(10, 2),
    year_built INTEGER,
    has_parking BOOLEAN,
    has_elevator BOOLEAN,
    has_storage BOOLEAN,
    has_balcony BOOLEAN,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(property_type, rooms, floor, size_sqm, year_built)
);

CREATE INDEX IF NOT EXISTS idx_dim_properties_type ON core.dim_properties (property_type);

-- Dimension: Time (for analytics)
CREATE TABLE IF NOT EXISTS core.dim_time (
    date_id DATE PRIMARY KEY,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR(20),
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR(20),
    is_weekend BOOLEAN
);

-- Fact: Historical Transactions (Gov.il data)
CREATE TABLE IF NOT EXISTS core.fact_transactions (
    transaction_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES core.dim_locations(location_id),
    property_id INTEGER REFERENCES core.dim_properties(property_id),
    transaction_date DATE NOT NULL,
    price DECIMAL(15, 2) NOT NULL,
    price_per_sqm DECIMAL(10, 2),
    deal_nature VARCHAR(50),  -- מכירה, מתנה, ירושה
    seller_type VARCHAR(50),
    buyer_type VARCHAR(50),
    source_id VARCHAR(100),   -- Original ID from Gov.il
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_transactions_date ON core.fact_transactions (transaction_date);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_location ON core.fact_transactions (location_id);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_price ON core.fact_transactions (price);

-- Fact: Current Listings (scraped data)
CREATE TABLE IF NOT EXISTS core.fact_listings (
    listing_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES core.dim_locations(location_id),
    property_id INTEGER REFERENCES core.dim_properties(property_id),
    source VARCHAR(50) NOT NULL,  -- Madlan, Yad2
    external_id VARCHAR(100),     -- ID from source
    listing_date DATE,
    asking_price DECIMAL(15, 2),
    price_per_sqm DECIMAL(10, 2),
    is_rental BOOLEAN DEFAULT FALSE,
    rental_price DECIMAL(10, 2),
    description TEXT,
    url VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(source, external_id)
);

CREATE INDEX IF NOT EXISTS idx_fact_listings_source ON core.fact_listings (source);
CREATE INDEX IF NOT EXISTS idx_fact_listings_active ON core.fact_listings (is_active);
CREATE INDEX IF NOT EXISTS idx_fact_listings_location ON core.fact_listings (location_id);

-- ============================================
-- ANALYTICS SCHEMA - Aggregated Metrics
-- ============================================

-- Aggregated ROI Metrics by Location
CREATE TABLE IF NOT EXISTS analytics.agg_roi_metrics (
    metric_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES core.dim_locations(location_id),
    city VARCHAR(100),
    neighborhood VARCHAR(100),
    calc_date DATE NOT NULL,
    -- Price metrics
    avg_price_sqm DECIMAL(10, 2),
    median_price_sqm DECIMAL(10, 2),
    min_price_sqm DECIMAL(10, 2),
    max_price_sqm DECIMAL(10, 2),
    transaction_count INTEGER,
    -- Trend metrics
    price_change_1m DECIMAL(5, 2),   -- % change vs 1 month ago
    price_change_3m DECIMAL(5, 2),   -- % change vs 3 months ago
    price_change_1y DECIMAL(5, 2),   -- % change vs 1 year ago
    -- Rental metrics
    avg_rental_price DECIMAL(10, 2),
    avg_rental_yield DECIMAL(5, 2),  -- Annual yield %
    -- ROI Score (composite)
    roi_score DECIMAL(5, 2),         -- 1-100 score
    risk_score DECIMAL(5, 2),        -- 1-100 score
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(location_id, calc_date)
);

CREATE INDEX IF NOT EXISTS idx_agg_roi_date ON analytics.agg_roi_metrics (calc_date);
CREATE INDEX IF NOT EXISTS idx_agg_roi_city ON analytics.agg_roi_metrics (city);
CREATE INDEX IF NOT EXISTS idx_agg_roi_score ON analytics.agg_roi_metrics (roi_score DESC);

-- Price History for Trend Analysis
CREATE TABLE IF NOT EXISTS analytics.price_history (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    neighborhood VARCHAR(100),
    calc_date DATE,
    avg_price_sqm DECIMAL(10, 2),
    transaction_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(city, neighborhood, calc_date)
);

-- Alerts Table
CREATE TABLE IF NOT EXISTS analytics.investment_alerts (
    alert_id SERIAL PRIMARY KEY,
    location_id INTEGER REFERENCES core.dim_locations(location_id),
    alert_type VARCHAR(50),  -- 'high_roi', 'price_drop', 'new_area'
    alert_message TEXT,
    roi_score DECIMAL(5, 2),
    is_read BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function to populate dim_time
CREATE OR REPLACE FUNCTION populate_dim_time(start_date DATE, end_date DATE)
RETURNS VOID AS $$
DECLARE
    curr_date DATE;
BEGIN
    curr_date := start_date;
    WHILE curr_date <= end_date LOOP
        INSERT INTO core.dim_time (
            date_id, year, quarter, month, month_name,
            week_of_year, day_of_month, day_of_week, day_name, is_weekend
        )
        VALUES (
            curr_date,
            EXTRACT(YEAR FROM curr_date),
            EXTRACT(QUARTER FROM curr_date),
            EXTRACT(MONTH FROM curr_date),
            TO_CHAR(curr_date, 'Month'),
            EXTRACT(WEEK FROM curr_date),
            EXTRACT(DAY FROM curr_date),
            EXTRACT(DOW FROM curr_date),
            TO_CHAR(curr_date, 'Day'),
            EXTRACT(DOW FROM curr_date) IN (0, 6)
        )
        ON CONFLICT (date_id) DO NOTHING;
        curr_date := curr_date + 1;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Populate time dimension for 2020-2030
SELECT populate_dim_time('2020-01-01', '2030-12-31');

-- ============================================
-- VIEWS FOR DASHBOARD
-- ============================================

-- View: Latest ROI by City
CREATE OR REPLACE VIEW analytics.v_latest_roi_by_city AS
SELECT DISTINCT ON (city)
    city,
    avg_price_sqm,
    price_change_1y,
    avg_rental_yield,
    roi_score,
    calc_date
FROM analytics.agg_roi_metrics
ORDER BY city, calc_date DESC;

-- View: Top Investment Opportunities
CREATE OR REPLACE VIEW analytics.v_top_opportunities AS
SELECT
    l.city,
    l.neighborhood,
    r.roi_score,
    r.avg_rental_yield,
    r.price_change_1y,
    r.avg_price_sqm,
    r.calc_date
FROM analytics.agg_roi_metrics r
JOIN core.dim_locations l ON r.location_id = l.location_id
WHERE r.calc_date = (SELECT MAX(calc_date) FROM analytics.agg_roi_metrics)
ORDER BY r.roi_score DESC
LIMIT 50;

COMMENT ON SCHEMA staging IS 'Raw data landing zone for ETL';
COMMENT ON SCHEMA core IS 'Dimensional model - Star Schema';
COMMENT ON SCHEMA analytics IS 'Aggregated metrics and views';
