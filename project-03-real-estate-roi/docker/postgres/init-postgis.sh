#!/bin/bash
set -e

# Enable PostGIS extension
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Enable PostGIS extension for geospatial queries
    CREATE EXTENSION IF NOT EXISTS postgis;
    CREATE EXTENSION IF NOT EXISTS postgis_topology;

    -- Verify PostGIS installation
    SELECT PostGIS_Version();

    -- Run schema creation script
    \i /docker-entrypoint-initdb.d/sql/create_tables.sql

    EOSQL

echo "PostGIS initialized successfully!"
