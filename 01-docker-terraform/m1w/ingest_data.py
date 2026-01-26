import click
import pandas as pd
from sqlalchemy import create_engine
from time import time


def ingest_parquet(
        url: str,
        engine,
        target_table: str,
        chunksize: int = 100000,
) -> None:
    """Ingest parquet data into PostgreSQL."""
    print(f"Downloading data from {url}...")
    t_start = time()
    
    
    df = pd.read_parquet(url)
    
    t_download = time()
    print(f"Downloaded {len(df)} rows in {t_download - t_start:.2f} seconds")
    print(f"Columns: {list(df.columns)}")
    
    # Insert data into PostgreSQL in chunks
    print(f"\nLoading data into table '{target_table}'...")
    
    for i in range(0, len(df), chunksize):
        chunk = df.iloc[i:i + chunksize]
        
        # First chunk replaces table, subsequent chunks append
        if_exists = 'replace' if i == 0 else 'append'
        
        chunk.to_sql(
            name=target_table,
            con=engine,
            if_exists=if_exists,
            index=False
        )
        
        print(f"  Inserted rows {i} to {min(i + chunksize, len(df))}")
    
    t_end = time()
    print(f"\nData ingestion complete!")
    print(f"Total rows inserted: {len(df)}")
    print(f"Total time: {t_end - t_start:.2f} seconds")

@click.command()
@click.option('--pg-user', default='postgres', help='PostgreSQL username')
@click.option('--pg-pass', default='postgres', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', default='5433', help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database name')
@click.option('--url', default='https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2025-11.parquet', help='URL of the parquet file')
@click.option('--chunksize', default=100000, type=int, help='Chunk size for data ingestion')
@click.option('--target-table', default='green_taxi_trips', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, url, chunksize, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    ingest_parquet(
        url=url,
        engine=engine,
        target_table=target_table,
        chunksize=chunksize
    )
    
    # Verify data
    result = pd.read_sql(f"SELECT COUNT(*) as count FROM {target_table}", engine)
    print(f"\nVerification: {result['count'].iloc[0]} rows in database")

if __name__ == '__main__':
    main()