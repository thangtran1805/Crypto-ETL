import duckdb

con = duckdb.connect('/home/thangtranquoc/crypto-etl-project/crypto-project/datawarehouse.duckdb')

con.sql('SELECT * FROM dim_coin;').show()
con.sql('SELECT COUNT(*) FROM dim_coin;').show()
con.sql('SELECT * FROM dim_time;').show()
con.sql('SELECT COUNT(*) FROM dim_time;').show()
con.sql('SELECT * FROM fact_market_cap;').show()
con.sql('SELECT COUNT(*) FROM fact_market_cap;').show()
con.sql('SELECT * FROM fact_price;').show()
con.sql('SELECT COUNT(*) FROM fact_price;').show()
con.sql('SELECT * FROM fact_supply;').show()
con.sql('SELECT COUNT(*) FROM fact_supply;').show()

