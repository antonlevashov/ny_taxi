import pandas as pd
import dask.dataframe as dd
import requests
import aiohttp
import pyarrow

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta
from dask.distributed import Client
from google.cloud import storage


""" 
To do:
1. Write local. Save in 10 small parque files first and read(clean) only the date column before uploading to GCS.
Save data in a new data folder
2. NEW Quality test. Add data quality and test function.
2. NEW Normalise data. Split in 3 parts.
3. Clean. Check the data types for  
2. Add parametrisation to the main function.

"""


@task(
    retries=0,
    tags=["fetch"],
    log_prints=True,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
    retry_delay_seconds=5,
)
def fetch(dataset_url: str, dataset_file: str) -> dd.DataFrame:
    """Read multistore data from web into dask DataFrame"""
    try:
        df = dd.read_csv(dataset_url)

    except requests.RequestException as e:
        raise RuntimeError(
            f"Failed to download the dataset from {dataset_url}. Error: {str(e)}"
        )
    except Exception as e:
        raise RuntimeError(
            f"Error occurred while processing the downloaded file. Error: {str(e)}"
        )

    return df


@task()
def local_fetch(dataset_file: str) -> dd.DataFrame:
    """Load local file to test the pipeline"""
    return None


@task()
def data_validation(df: dd.DataFrame) -> dd.DataFrame:
    """Checks the data"""
    # https://docs.greatexpectations.io/docs/tutorials/quickstart/

    return None


@task(log_prints=True)
def clean(df: dd.DataFrame) -> dd.DataFrame:
    """Fix dtype issues"""
    df["event_time"] = dd.to_datetime(df["event_time"])
    # Add more transformations

    return df


@task()
def write_local(df: dd.DataFrame, dataset_file: str) -> None:
    """Write DataFrame out locally as parquet file"""
    local_path = Path(f"../../../data/multistore/{dataset_file}.parquet")

    # Check if the file already exists locally
    if not local_path.exists():
        # Split the file into 10 parts
        df.repartition(10).to_parquet(local_path, compression="gzip")


@task()
def local_to_gcs(local_path: Path, dataset_file: str) -> None:
    """Upload local parquet file to GCS using resumable uploads"""

    # Reference the bucket and blob (file)
    gcs_block = GcsBucket.load("ecommerce-gcs")
    gcs_path = Path(f"multistore/{dataset_file}.parquet").as_posix()
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)

    # # Check if the file already exists in GCS
    # if not blob.exists():
    #     # Upload the file with a resumable session
    return


@task()
def df_to_gcs(df: dd.DataFrame, dataset_file: str) -> None:
    """Write the parquet file to GCS using resumable uploads"""

    # Reference the bucket and blob (file)
    gcs_block = GcsBucket.load("ecommerce-gcs")
    gcs_path = Path(f"multistore/{dataset_file}.parquet").as_posix()
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)
    return


@flow(log_prints=True)
def etl_web_to_gcs(dataset_file: str) -> None:
    """The main ETL function"""
    dataset_url = f"https://data.rees46.com/datasets/marketplace/{dataset_file}.csv.gz"
    df = fetch(dataset_url, dataset_file)
    write_local(df, dataset_file)

    # df_clean = clean(df)
    # local_path = write_local(df_clean, dataset_file)
    # local_to_gcs(local_path, dataset_file)


@flow()
def etl_parent_flow(months: list) -> None:
    # Initialize a Dask client to control and monitor computation.
    client = Client()

    for month in months:
        etl_web_to_gcs(month)


if __name__ == "__main__":
    months = [
        "2019-Oct",
        "2019-Nov",
        "2019-Dec",
        "2020-Jan",
        "2020-Feb",
        "2020-Mar",
        "2020-Apr"
    ]
    etl_parent_flow(months)
