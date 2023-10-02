from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp import GcpCredentials


@task(retries=3)
def extract_from_gcs(name: str) -> Path:
    """Download trip data from GCS"""
    gcs_path = f"multistore/{name}.parquet"
    gcs_block = GcsBucket.load("ecommerce-gcs")
    gcs_block.get_directory(from_path=gcs_path, local_path=f"../")
    return Path(f"../{gcs_path}").as_posix()


@task()
def transform(path: Path) -> pd.DataFrame:
    """Data cleaning example"""
    df = pd.read_parquet(path)

    print(f"Number of columns before:{len(df.columns)}")

    # Split by the first period (n=1) and drop category_code
    df[["main_category", "sub_category"]] = df["category_code"].str.split(
        ".", n=1, expand=True
    )
    df = df.drop(columns=["category_code"])

    print(f"Number of columns after:{len(df.columns)}")

    # Rearrange the columns
    new_order = [
        "event_time",
        "event_type",
        "product_id",
        "category_id",
        "main_category",
        "sub_category",
        "brand",
        "price",
        "user_id",
        "user_session",
    ]
    df = df[new_order]
    print(f"New columns:{df.columns[0]}")

    return df


@task()
def write_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to BiqQuery"""

    gcp_credentials_block = GcpCredentials.load("ecommerce-gcp-creds")

    df.to_gbq(
        destination_table="all_data.october",
        project_id="ecommerce-398407",
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists="append",
    )


@flow()
def etl_gcs_to_bq():
    """Main ETL flow to load data into Big Query"""
    name = "2019-Oct"

    path = extract_from_gcs(name)
    df = transform(path)
    write_bq(df)


if __name__ == "__main__":
    etl_gcs_to_bq()
