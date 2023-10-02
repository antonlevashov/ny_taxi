from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from random import randint
from prefect.tasks import task_input_hash
from datetime import timedelta
import requests


@task(
    retries=3,
    retry_delay_seconds=5,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(minutes=150),
)
def fetch(dataset_url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas DataFrame"""
    df = pd.read_csv(dataset_url)
    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
    df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])
    print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


@task()
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write DataFrame out locally as parquet file"""
    Path(f"taxi_data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"taxi_data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")
    return path


@task()
def write_gcs(path: Path) -> None:
    """Upload local parquet file to GCS"""
    gcs_block = GcsBucket.load("ecommerce-gcs")
    gcs_block.upload_from_path(from_path=path, to_path=path.as_posix())


@task()
def stream_to_gcs(df: pd.DataFrame, gcs_path: Path) -> None:
    """Stream a pandas DataFrame to GCS as a parquet file."""
    gcs_bucket = GcsBucket.load("ecommerce-gcs")
    gcs_bucket.upload_from_dataframe(
        df=df, to_path=gcs_path, serialization_format="parquet_gzip"
    )


@flow(log_prints=True)
def etl_web_to_gcs(year: int, month: int, color: str) -> None:
    """The main ETL function"""
    dataset_file = f"{color}_tripdata_{year}-{month:02}"
    dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

    print(f"Fetching: {dataset_url}.")
    df = fetch(dataset_url)
    df_clean = clean(df)
    gcs_path = Path(f"taxi_data/{color}/{dataset_file}.parquet").as_posix()
    stream_to_gcs(df_clean, gcs_path)

    # path = write_local(df_clean, color, dataset_file)
    # write_gcs(path)


@flow(name="Flow Name Taxi Data", log_prints=True)
def etl_parent_flow(months: list[int], years: list[int], colors: list[str]) -> None:
    for year in years:
        for month in months:
            for color in colors:
                dataset_file = f"{color}_tripdata_{year}-{month:02}"
                dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

                # Check the HTTP code and skip if other than 200
                # response = requests.head(dataset_url)

                # if response.status_code != 200:
                #     print(f"URL does not exist: {dataset_url}. Skipping.")

                try:
                    etl_web_to_gcs(year, month, color)

                except pd.errors.ParserError:
                    # Handle issues with the CSV format related to Pandas
                    print(f"Error parsing the CSV from URL: {dataset_url}")
                    raise

                except Exception as e:
                    # Handle other generic errors
                    print(f"Error fetching data from URL: {dataset_url}. Error: {e}")
                    raise


if __name__ == "__main__":
    colors = ["yellow", "green", "fhv", "fhvhv"]
    months = [x for x in range(1, 13)]
    years = [2019, 2020, 2021]

    # etl_parent_flow.serve(months, year, color)
    etl_parent_flow.serve(
        name="taxi_data_deployment2",
        tags=["taxi_data_tag"],
        parameters={"years": years, "months": months, "colors": colors},
    )
