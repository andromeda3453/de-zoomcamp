from stat import FILE_ATTRIBUTE_ENCRYPTED
import pandas as pd
from time import time
from sqlalchemy import create_engine
import argparse
import os
import pyarrow.parquet as pq

def main(params):
    
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    
    file_type = url.split(".")[-1]
    
    file_name = f'output.{file_type.lower()}'
    
    # This will run the wget command to download the parquet from the given url and save it under the name output.parquet/output.csv
    os.system(f'wget {url} --no-check-certificate -O {file_name}')
    
    # We need to create a connection to postgres to run the DDL command to create the table for the taxi data
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # We will load the data into the database in chunks because there is a large number of records
    # we can get an iterator from the read_csv function by passing true to the iterator arg.
    # df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    if file_type.lower() == 'parquet':
        parquet = pq.ParquetFile(file_name)
        iter = parquet.iter_batches(batch_size=100000)
        df = next(iter).to_pandas()
    else:
        iter = pd.read_csv(file_name, iterator=True, chunksize=100000)
        df = next(iter)
    

    if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
        #convert pickup and dropoff times to datetime type
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)

    #Check the schema of the dataframe
    # print(pd.io.sql.get_schema(df, name='yellow_taxi_data', con=engine))
    
    print('Loading data into database...\n\n')

    # the to_sql function inserts the data in the df into the database. 
    # if we execute this function on df.head(n=0) then it will only create the table but not insert any data
    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
        
        
    # code to insert data chunkwise
    for chunk in iter:
        
        t_start = time()
        
        # get next chunk
        df = chunk.to_pandas()
        
        if "tpep_pickup_datetime" in df.columns and "tpep_dropoff_datetime" in df.columns:
            #convert pickup and dropoff times to datetime type
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_name, con=engine, if_exists='append')
        
        
        t_end = time()
        print(f"added another chunk... took {t_end - t_start} seconds")
    
    
#this is used when we want to run code as a script
if __name__ == '__main__':

    '''
    List of parameters we need to accept:
    user
    password
    host
    port
    database name
    table name
    csv url
    '''
    parser = argparse.ArgumentParser(description='Ingest CSV data into Postgres')

    parser.add_argument('--user', help='username to use to access postgres')
    parser.add_argument('--password', help='password to use to access postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database to use in postgres')
    parser.add_argument('--table_name', help='table that data will be written into in the given database')
    parser.add_argument('--url', help='url to download csv file from')

    args = parser.parse_args()
    
    main(args)











