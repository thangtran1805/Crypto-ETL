import duckdb
import os

# duckdb file path
database_path  = r"/home/thangtranquoc/crypto-etl-project/crypto-project/datawarehouse.duckdb"

# Remove database if already exists
if os.path.exists(database_path):
    os.remove(database_path)

# Connect to duckdb, open or create database
conn = duckdb.connect(database=database_path)

# Read the query create schema file
with open(r'/home/thangtranquoc/crypto-etl-project/crypto-project/sql/config_dw/schema/schema.sql','r') as file:
    query = file.read()

# Execute the query
conn.execute(query)

# Close the connection
conn.close()

# Print the message
print(f'Database has been created to {database_path}')

