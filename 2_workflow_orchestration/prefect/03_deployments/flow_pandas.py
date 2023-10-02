import pandas as pd
import os

from pathlib import Path
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect.tasks import task_input_hash
from datetime import timedelta


@task(
    log_prints=True,
    tags=["fetch"],
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def fetch(dataset_url: str, dataset_file: str) -> pd.DataFrame:
    """Read multistore data from web into dask DataFrame"""
    local_path = Path(f"multistore/{dataset_file}.parquet").resolve()

    # Check if the file already exists locally
    if local_path.exists():
        df = pd.read_parquet(local_path, engine="pyarrow")
    else:
        df = pd.read_csv(dataset_url)

    return df


@task(log_prints=True)
def clean(df: pd.DataFrame) -> pd.DataFrame:
    """Fix dtype issues"""
    print(df.head(5))
    # df["event_time"] = pd.to_datetime(df["event_time"])
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")

    return df


def df_to_parquet(
    df: pd.DataFrame,
    target_dir: Path,
    dataset_file: str,
    chunk_size=1000000,
    **parquet_wargs,
) -> None:
    """Writes pandas DataFrame to parquet format with pyarrow.

    Args:
        df: DataFrame
        target_dir: local directory where parquet files are written to
        chunk_size: number of rows stored in one chunk of parquet file. Defaults to 1000000.
    """
    for i in range(0, len(df), chunk_size):
        slc = df.iloc[i : i + chunk_size]
        chunk = int(i / chunk_size) + 1
        fname = os.path.join(target_dir, f"{dataset_file}_{chunk:02d}.parquet")
        slc.to_parquet(fname, engine="pyarrow", **parquet_wargs)


@task()
def write_local(df: pd.DataFrame, dataset_file: str) -> None:
    """Write DataFrame out locally as parquet file"""
    local_path = Path(f"multistore/{dataset_file}").resolve()
    if not os.path.exists(local_path):
        os.makedirs(local_path)

    df_to_parquet(df, local_path, dataset_file, 5000000)


@task()
def write_gcs(local_path: Path, dataset_file: str) -> None:
    """Upload local parquet file to GCS using resumable uploads."""
    gcs_path = Path(f"multistore/{dataset_file}.parquet").as_posix()
    # Reference the bucket and blob (file)
    gcs_block = GcsBucket.load("ecommerce-gcs")
    gcs_block.upload_from_path(from_path=local_path, to_path=gcs_path)


@task(log_prints=True)
def stream_to_gcs(df: pd.DataFrame, gcs_path: Path) -> None:
    """Stream a pandas DataFrame to GCS as a parquet file."""
    gcs_bucket = GcsBucket.load("ecommerce-gcs")
    gcs_bucket.upload_from_dataframe(
        df=df, to_path=gcs_path, serialization_format="parquet"
    )


@flow(log_prints=True)
def etl_web_to_gcs(dataset_file: str) -> None:
    """The main ETL function"""
    dataset_url = f"https://data.rees46.com/datasets/marketplace/{dataset_file}.csv.gz"
    df = fetch(dataset_url, dataset_file)
    write_local(df, dataset_file)

    # gcs_path = Path(f"multistore/{dataset_file}.parquet").as_posix()
    # stream_to_gcs(df, gcs_path)


@flow(name="Multistore", log_prints=True)
def etl_parent_flow(months: list) -> None:
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
        "2020-Apr",
    ]

    etl_parent_flow.serve(name="multistore_deployment", parameters={"months": months})

    # etl_parent_flow(months)
