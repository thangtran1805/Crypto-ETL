# 📄 Table Schema Change Log

This file tracks all changes to the data warehouse schema over time.

---

## ✅ Initial Schema (2025-07-15)

### DIMENSION TABLES

#### `dim_coin`
| Column Name  | Data Type | Description          |
|--------------|------------|----------------------|
| coin_id      | INTEGER    | Auto-incremented PK  |
| coin_symbol  | TEXT       | Symbol (e.g., BTC)   |
| coin_name    | TEXT       | Full coin name       |

#### `dim_time`
| Column Name  | Data Type | Description          |
|--------------|------------|----------------------|
| time_id      | INTEGER    | Auto-incremented PK  |
| date         | DATE       | Snapshot date        |
| year         | INTEGER    | Extracted from date  |
| month        | VARCHAR    | Month name           |
| quarter      | VARCHAR    | Quarter (Q1..Q4)     |
| day_of_week  | VARCHAR    | Name of weekday      |

---

### FACT TABLES

#### `fact_price`
| Column Name                | Description                          |
|---------------------------|--------------------------------------|
| coin_id             | FK to `dim_coin(coin_id)`            |
| time_id             | FK to `dim_time(time_id)`            |
| current_price             | Current trading price                |
| high_price_24h            | Highest price in past 24h            |
| low_price_24h             | Lowest price in past 24h             |
| price_change_24h          | Net change in price over 24h         |
| price_change_percentage_24h | Percentage change over 24h        |
| total_volume              | Total trading volume                 |

#### `fact_market_cap`
| Column Name                   | Description                        |
|------------------------------|------------------------------------|
| coin_id               | FK to `dim_coin(coin_id)`          |
| time_id               | FK to `dim_time(time_id)`          |
| market_cap                   | Market capitalization              |
| market_cap_rank              | Rank based on market cap           |
| market_fully_diluted_valuation | Fully diluted market valuation  |
| market_cap_change_24h        | Change in market cap in 24h        |
| market_cap_change_percentage_24h | Percentage change             |

#### `fact_supply`
| Column Name        | Description                                |
|--------------------|--------------------------------------------|
| coin_id     | FK to `dim_coin(coin_id)`                  |
| time_id     | FK to `dim_time(time_id)`                  |
| circulating_supply | Coins currently circulating                |
| total_supply       | Total coins created so far                 |
| max_supply         | Maximum possible coins                     |

---

## 🔄 Planned Changes

- Add `fact_ath_atl` table (tracking all-time high/low values)
- Add `dim_exchange` if exchange data is ingested