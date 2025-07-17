-- Tạo sequence cho các bảng
CREATE SEQUENCE time_id_seq;
CREATE SEQUENCE coin_id_seq;

--dim_coin
CREATE TABLE IF NOT EXISTS dim_coin (
    coin_id INTEGER DEFAULT NEXTVAL('coin_id_seq') PRIMARY KEY,
    coin_symbol TEXT,
    coin_name TEXT
);


-- dim_time
CREATE TABLE IF NOT EXISTS dim_time (
    time_id INTEGER DEFAULT NEXTVAL('time_id_seq') PRIMARY KEY,
    date DATE NOT NULL,
    year INTEGER,
    month VARCHAR(10),
    quarter VARCHAR(10),
    day_of_week VARCHAR (10)
);

--fact_price
CREATE TABLE IF NOT EXISTS fact_price (
    coin_id INTEGER,
    time_id INTEGER,
    current_price DOUBLE NOT NULL,
    high_price_24h DOUBLE NOT NULL,
    low_price_24h DOUBLE NOT NULL,
    price_change_24h DOUBLE NOT NULL,
    price_change_percentage_24h DOUBLE NOT NULL,
    total_volume BIGINT NOT NULL,
    FOREIGN KEY (coin_id) REFERENCES dim_coin(coin_id),
    FOREIGN KEY (time_id) REFERENCES dim_time(time_id)
);

-- fact_market_cap
CREATE TABLE IF NOT EXISTS fact_market_cap (
    coin_id INTEGER,
    time_id INTEGER,
    market_cap DOUBLE NOT NULL,
    market_cap_rank INTEGER NOT NULL,
    market_fully_diluted_valuation DOUBLE NOT NULL,
    market_cap_change_24h DOUBLE NOT NULL,
    market_cap_change_percentage_24h DOUBLE NOT NULL,
    FOREIGN KEY(coin_id) REFERENCES dim_coin(coin_id),
    FOREIGN KEY(time_id) REFERENCES dim_time(time_id)
);

-- fact_supply
CREATE TABLE IF NOT EXISTS fact_supply(
    coin_id INTEGER,
    time_id INTEGER,
    circulating_supply DOUBLE NOT NULL,
    total_supply DOUBLE NOT NULL,
    max_supply DOUBLE,
    FOREIGN KEY(coin_id) REFERENCES dim_coin(coin_id),
    FOREIGN KEY(time_id) REFERENCES dim_time(time_id)
)