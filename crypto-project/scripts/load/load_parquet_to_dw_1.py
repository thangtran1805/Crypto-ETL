import duckdb
import pyarrow.parquet as pq
import os
con = duckdb.connect('/home/thangtranquoc/crypto-etl-project/crypto-project/datawarehouse.duckdb')

def get_latest_file_in_directory(directory,extension):
    """
    Get the latest file in a directory.
    :param: directory: A directory to search for files.
    :param: extension: File extension to search for.
    :return: Latest file in directory.
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]

    # Return None if no files were found
    if not files:
        return None
    
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    extension = '.parquet'
    latest_file = get_latest_file_in_directory(directory,extension)
    if latest_file:
        print(f'Reading: {latest_file}')
        return pq.read_table(latest_file)
    else:
        print(f'No parquet files were found in {directory}')
        return None
    
def load_dim_tables():
    dim_base_path = '/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/processed/dim'

    dim_tables = {
        'dim_coin' : {
            'path' : os.path.join(dim_base_path,'dim_coin'),
            'columns' : ['coin_symbol','coin_name']
        },
        'dim_time' : {
            'path' : os.path.join(dim_base_path,'dim_time'),
            'columns' : ['date','year','month','quarter','day_of_week']
        }
    }
    for table_name, config in dim_tables.items():
        folder_path = config['path']
        columns = config['columns']
        table = read_latest_file_in_directory(folder_path)
        if table:
            con.register('arrow_table',table)
            col_str = ', '.join(columns)
            query = f"""
                    INSERT INTO {table_name} ({col_str})
                    SELECT {col_str} FROM arrow_table
            """
            con.execute(query)
            print(f'Inserted data into datawarehouse: {table_name}')
        else:
            print(f'Failed to load data into datawarehouse : {table_name}')
    print('Data has been successfully inserted into datawarehouse!')
def load_parquet_to_dw_1():
    print('Loading dims tables to datawarehouse...')
    load_dim_tables()
# load_parquet_to_dw_1()