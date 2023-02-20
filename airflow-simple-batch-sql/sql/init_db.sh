#!/bin/bash
set -e
export PGPASSWORD=$POSTGRES_PASSWORD;
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
  CREATE USER $DATA_DB_USER WITH PASSWORD '$DATA_DB_PASS';
  CREATE DATABASE $DATA_DB_NAME;
  GRANT ALL PRIVILEGES ON DATABASE $DATA_DB_NAME TO $DATA_DB_USER;
  \connect $DATA_DB_NAME $DATA_DB_USER
  BEGIN;
    CREATE TABLE IF NOT EXISTS assets (
      id VARCHAR(50) NOT NULL,
      rank INT NOT NULL,
      symbol VARCHAR(5) NOT NULL,
      name VARCHAR(50) NOT NULL,
      supply FLOAT8 NOT NULL,
      max_supply FLOAT8, 
      market_cap_usd FLOAT8 NOT NULL,
      volume_usd_24hr FLOAT8 NOT NULL,
      price_usd FLOAT8 NOT NULL,
      change_pct_24hr FLOAT8 NOT NULL,
      vwap_24hr FLOAT8,
      explorer VARCHAR(150),
      execution_ts TIMESTAMPTZ,
      PRIMARY KEY (execution_ts, id)
	);
  COMMIT;
EOSQL
