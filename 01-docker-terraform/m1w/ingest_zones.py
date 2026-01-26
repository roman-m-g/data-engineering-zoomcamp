import click
import pandas as pd
from sqlalchemy import create_engine
from time import time


def ingest_csv(
        url: str,
        engine,
        target_table: str,
) -> None:
    """Ingest CSV data into PostgreSQL."""
    print(f"Downloading data from {url}...")
    t_start = time()
    
    # Read CSV file
    df = pd.read_csv(url)
    
    t_download = time()
    print(f"Downloaded {len(df)} rows in {t_download - t_start:.2f} seconds")
    print(f"Columns: {list(df.columns)}")
    print(f"\nData preview:\n{df.head()}")
    
    # Insert data into PostgreSQL
    print(f"\nLoading data into table '{target_table}'...")
    
    df.to_sql(
        name=target_table,
        con=engine,
        if_exists='replace',
        index=False
    )
    
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
@click.option('--url', default='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv', help='URL of the CSV file')
@click.option('--target-table', default='zones', help='Target table name')
def main(pg_user, pg_pass, pg_host, pg_port, pg_db, url, target_table):
    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    ingest_csv(
        url=url,
        engine=engine,
        target_table=target_table,
    )
    
    # Verify data
    result = pd.read_sql(f"SELECT COUNT(*) as count FROM {target_table}", engine)
    print(f"\nVerification: {result['count'].iloc[0]} rows in database")


if __name__ == '__main__':
    main()