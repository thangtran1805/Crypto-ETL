from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,year,month,quarter,dayofweek,to_date,to_timestamp
import json
import shutil
import os
import pandas as pd
from datetime import datetime
def get_latest_file_in_directory(directory,extension):
    """
    Get the latest file in a directory with a specific extension
    :param directory: A directory to search for file.
    :param extension: Extension of file to search for
    :return latest file
    """
    files = [os.path.join(directory,f) for f in os.listdir(directory) if f.endswith(extension)]
    if not files:
        return None
    
    latest_file = max(files,key=os.path.getmtime)
    return latest_file

def read_latest_file_in_directory(directory):
    extension = '.json'
    latest_file = get_latest_file_in_directory(directory,extension)
    if latest_file:
        with open(latest_file,'r') as file:
            data_json = json.load(file)
        print(f'Transforming from file: {latest_file}')
    else:
        print('No file were found!')
        data_json = []
    return data_json

def write_named_parquet(df, folder_path, file_name):
    """
    Ghi DataFrame thành 1 file parquet có tên cố định bên trong thư mục riêng
    """
    import uuid
    temp_dir = os.path.join(folder_path, f"temp_{uuid.uuid4()}")
    df.coalesce(1).write.mode("overwrite").parquet(temp_dir)

    # Tạo thư mục đích nếu chưa có
    os.makedirs(folder_path, exist_ok=True)

    # Đổi tên file .parquet trong thư mục tạm
    for file in os.listdir(temp_dir):
        if file.endswith(".parquet"):
            src = os.path.join(temp_dir, file)
            dst = os.path.join(folder_path, file_name)
            os.rename(src, dst)
            break

    shutil.rmtree(temp_dir)

def transform_data(data_json):
    if not data_json:
        print('No data to transform')
        return
    
    # Create Spark Session
    spark = SparkSession.builder \
        .appName('TransformCryto') \
        .getOrCreate()

    # Take today date to make time key
    today_str = datetime.today().strftime('%Y_%m_%d')

    output_dir = r'/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/processed'

    # Create Spark Dataframe from JSON file
    df = spark.createDataFrame(pd.DataFrame(data_json))
    df = df.withColumn('date',to_date(to_timestamp('last_updated')))

    # dim_coin
    dim_coin = df.select(
        col('symbol').alias('coin_symbol'),
        col('name').alias('coin_name')
    ).dropDuplicates(['coin_symbol'])

    # dim_time
    dim_time = df.select(
        col('date'),
        year('date').alias('year'),
        month('date').alias('month'),
        quarter('date').alias('quarter'),
        dayofweek('date').alias('day_of_week')
    ).dropDuplicates(['date'])

    # fact_price
    fact_price = df.select(
        col('symbol').alias('coin_symbol'),
        col('date'),
        col('current_price').alias('current_price'),
        col('high_24h').alias('high_price_24h'),
        col('low_24h').alias('low_price_24h'),
        col('price_change_24h').alias('price_change_24h'),
        col('price_change_percentage_24h'),
        col('total_volume').cast('bigint')
    )

    # fact_market_cap
    fact_market_cap = df.select(
        col('symbol').alias('coin_symbol'),
        col('date'),
        col('market_cap'),
        col('market_cap_rank'),
        col('fully_diluted_valuation').alias('market_fully_diluted_valuation'),
        col('market_cap_change_24h'),
        col('market_cap_change_percentage_24h')
    )

    # fact_supply
    fact_supply = df.select(
        col('symbol').alias('coin_symbol'),
        col('date'),
        col('circulating_supply'),
        col('total_supply'),
        col('max_supply')
    )
    # Ghi từng bảng ra file parquet
    write_named_parquet(dim_coin, os.path.join(output_dir, "dim/dim_coin"), "dim_coin_" + f'{today_str}.parquet')
    write_named_parquet(dim_time, os.path.join(output_dir, "dim/dim_time"), "dim_time_" + f'{today_str}.parquet')
    write_named_parquet(fact_price, os.path.join(output_dir, "fact/fact_price"), "fact_price_" + f"{today_str}.parquet")
    write_named_parquet(fact_market_cap, os.path.join(output_dir, "fact/fact_market_cap"), "fact_market_cap_" + f"{today_str}.parquet")
    write_named_parquet(fact_supply, os.path.join(output_dir, "fact/fact_supply"), "fact_supply_" + f"{today_str}.parquet")
    spark.stop()
    print(f" Transform completed for {today_str} and saved to {output_dir}/")
    
def transform_to_parquet():
    print('Transforming to Parquet')
    raw_dir = r'/home/thangtranquoc/crypto-etl-project/crypto-project/backend/data/raw'
    data_json = read_latest_file_in_directory(raw_dir)
    transform_data(data_json)
    print('All tasks are completed')

# transform_to_parquet()