import duckdb
import pyarrow.parquet as pq
import os
from datetime import datetime

# Kết nối tới DuckDB
con = duckdb.connect('/home/thangtranquoc/crypto-etl-project/crypto-project/datawarehouse.duckdb')

def get_latest_file_in_directory(directory, extension):
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    return max(files, key=os.path.getmtime) if files else None

def read_latest_file_in_directory(directory):
    extension = '.parquet'
    latest_file = get_latest_file_in_directory(directory, extension)
    if latest_file:
        print(f' Reading: {latest_file}')
        return pq.read_table(latest_file)
    else:
        print(f'No parquet files were found in {directory}')
        return None

def load_fact_tables():
    fact_base_path = '/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/processed/fact'

    fact_tables = {
        'fact_market_cap': {
            'path': os.path.join(fact_base_path, 'fact_market_cap'),
            'columns': [
                'coin_id', 'time_id',
                'market_cap', 'market_cap_rank',
                'market_fully_diluted_valuation',
                'market_cap_change_24h', 'market_cap_change_percentage_24h'
            ]
        },
        'fact_price': {
            'path': os.path.join(fact_base_path, 'fact_price'),
            'columns': [
                'coin_id', 'time_id',
                'current_price', 'high_price_24h', 'low_price_24h',
                'price_change_24h', 'price_change_percentage_24h',
                'total_volume'
            ]
        },
        'fact_supply': {
            'path': os.path.join(fact_base_path, 'fact_supply'),
            'columns': [
                'coin_id', 'time_id',
                'circulating_supply', 'total_supply', 'max_supply'
            ]
        }
    }

    # Load dim tables để join
    dim_coin = con.execute('SELECT coin_id, coin_symbol FROM dim_coin').fetch_arrow_table()
    dim_time = con.execute('SELECT time_id, date FROM dim_time').fetch_arrow_table()

    for table_name, config in fact_tables.items():
        folder_path = config['path']
        columns = config['columns']
        table = read_latest_file_in_directory(folder_path)

        if table:
            # Register temp tables
            con.register('fact_temp', table)
            con.register('dim_coin_temp', dim_coin)
            con.register('dim_time_temp', dim_time)

            # Tạo thư mục chứa bad rows nếu cần
            error_output_dir = '/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/bad_rows'
            os.makedirs(error_output_dir, exist_ok=True)
            today_str = datetime.today().strftime('%Y_%m_%d')
            bad_rows_file = os.path.join(error_output_dir, f'bad_rows_{table_name}_{today_str}.parquet')

            # Lấy các dòng không join được (bad rows)
            bad_rows = con.execute(f"""
                SELECT *
                FROM fact_temp f
                LEFT JOIN dim_coin_temp dc ON f.coin_symbol = dc.coin_symbol
                LEFT JOIN dim_time_temp dt ON f.date = dt.date
                WHERE dc.coin_id IS NULL OR dt.time_id IS NULL
            """).fetch_arrow_table()

            if bad_rows.num_rows > 0:
                pq.write_table(bad_rows, bad_rows_file)
                print(f'  {bad_rows.num_rows} bad rows saved to: {bad_rows_file}')

            # Chèn dữ liệu vào bảng fact, tránh trùng (coin_id, time_id)
            col_str = ', '.join(columns)
            query = f"""
                INSERT INTO {table_name} ({col_str})
                SELECT
                    dcoin.coin_id,
                    dtime.time_id,
                    f.*
                EXCLUDE (coin_symbol, date)
                FROM fact_temp f
                INNER JOIN dim_coin dcoin ON f.coin_symbol = dcoin.coin_symbol
                INNER JOIN dim_time dtime ON f.date = dtime.date
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} existing
                    WHERE existing.coin_id = dcoin.coin_id
                      AND existing.time_id = dtime.time_id
                )
            """
            con.execute(query)
            print(f' Inserted data into datawarehouse: {table_name}')
        else:
            print(f'  Skipped {table_name} due to missing data')

    print(' All fact tables have been loaded into the data warehouse.')

def load_parquet_to_dw_2():
    print(' Loading fact tables into DuckDB data warehouse...')
    load_fact_tables()

# Gọi để chạy nếu cần:
# load_parquet_to_dw_2()
