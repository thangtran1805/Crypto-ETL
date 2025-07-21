import duckdb
import pyarrow.parquet as pq
import os

# Connect to DuckDB
con = duckdb.connect('/home/thangtranquoc/crypto-etl-project/crypto-project/datawarehouse.duckdb')

def get_latest_file_in_directory(directory, extension):
    """
    Get the latest file in a directory with a given extension.
    """
    files = [os.path.join(directory, f) for f in os.listdir(directory) if f.endswith(extension)]
    return max(files, key=os.path.getmtime) if files else None

def read_latest_file_in_directory(directory):
    extension = '.parquet'
    latest_file = get_latest_file_in_directory(directory, extension)
    if latest_file:
        print(f' Reading: {latest_file}')
        return pq.read_table(latest_file)
    else:
        print(f'⚠️  No parquet files were found in {directory}')
        return None

def load_dim_tables():
    dim_base_path = '/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/processed/dim'

    dim_tables = {
        'dim_coin': {
            'path': os.path.join(dim_base_path, 'dim_coin'),
            'columns': ['coin_symbol', 'coin_name']
        },
        'dim_time': {
            'path': os.path.join(dim_base_path, 'dim_time'),
            'columns': ['date', 'year', 'month', 'quarter', 'day_of_week']
        }
    }

    for table_name, config in dim_tables.items():
        folder_path = config['path']
        columns = config['columns']
        table = read_latest_file_in_directory(folder_path)

        if table:
            con.register('arrow_table', table)
            col_str = ', '.join(columns)

            # Build WHERE NOT EXISTS join condition
            join_condition = ' AND '.join([f'f.{col} = d.{col}' for col in columns])

            query = f"""
                INSERT INTO {table_name} ({col_str})
                SELECT {col_str}
                FROM arrow_table f
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table_name} d
                    WHERE {join_condition}
                )
            """
            try:
                con.execute(query)
                print(f' Inserted data into datawarehouse: {table_name}')
            except Exception as e:
                print(f' Failed to insert into {table_name}: {e}')
        else:
            print(f'  Skipped loading {table_name} due to missing data.')

    print('✅ All dim tables loaded into datawarehouse.')

def load_parquet_to_dw_1():
    print('🚀 Loading dimension tables into DuckDB data warehouse...')
    load_dim_tables()

# Run 
# load_parquet_to_dw_1()