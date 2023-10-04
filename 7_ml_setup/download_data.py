from pathlib import Path
import pandas as pd
from random import randint
from itertools import product
import requests


# This is a simplified veriosn of the Prefect flow to donaload the NY dat alocaly


def fetch(dataset_url: str) -> pd.DataFrame:
    print(dataset_url)
    df = pd.read_csv(dataset_url, compression="gzip")
    return df


def clean(color: str, df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean all 4 datasets.
    """

    if color == "yellow":
        """Fix dtype issues"""
        df["tpep_pickup_datetime"] = pd.to_datetime(df["tpep_pickup_datetime"])
        df["tpep_dropoff_datetime"] = pd.to_datetime(df["tpep_dropoff_datetime"])

    if color == "green":
        """Fix dtype issues"""
        df["lpep_pickup_datetime"] = pd.to_datetime(df["lpep_pickup_datetime"])
        df["lpep_dropoff_datetime"] = pd.to_datetime(df["lpep_dropoff_datetime"])
        df["trip_type"] = df["trip_type"].astype("Int64")

    if color == "yellow" or color == "green":
        df["VendorID"] = df["VendorID"].astype("Int64")
        df["RatecodeID"] = df["RatecodeID"].astype("Int64")
        df["PULocationID"] = df["PULocationID"].astype("Int64")
        df["DOLocationID"] = df["DOLocationID"].astype("Int64")
        df["passenger_count"] = df["passenger_count"].astype("Int64")
        df["payment_type"] = df["payment_type"].astype("Int64")

    if color == "fhv":
        """Rename columns"""
        df.rename(
            {"dropoff_datetime": "dropOff_datetime"}, axis="columns", inplace=True
        )
        df.rename({"PULocationID": "PUlocationID"}, axis="columns", inplace=True)
        df.rename({"DOLocationID": "DOlocationID"}, axis="columns", inplace=True)

        """Fix dtype issues"""
        df["pickup_datetime"] = pd.to_datetime(df["pickup_datetime"])
        df["dropOff_datetime"] = pd.to_datetime(df["dropOff_datetime"])

        # See https://pandas.pydata.org/docs/user_guide/integer_na.html
        df["PUlocationID"] = df["PUlocationID"].astype("Int64")
        df["DOlocationID"] = df["DOlocationID"].astype("Int64")

    # print(df.head(2))
    print(f"columns: {df.dtypes}")
    print(f"rows: {len(df)}")
    return df


def write_local(color: str, df: pd.DataFrame, dataset_file: str) -> None:
    """Write DataFrame out locally as csv file"""
    Path(f"../data/{color}").mkdir(parents=True, exist_ok=True)
    path = Path(f"../data/{color}/{dataset_file}.parquet")
    df.to_parquet(path, compression="gzip")


if __name__ == "__main__":
    colors = ["green", "yellow", "fhv"]
    years = [2019, 2020, 2021]
    months = range(1, 13)

    for color, year, month in product(colors, years, months):
        dataset_file = f"{color}_tripdata_{year}-{month:02}"
        dataset_url = f"https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz"

        # Check if URL is valid using GET request
        with requests.get(dataset_url, stream=True) as response:
            if response.status_code == 200:
                df = fetch(dataset_url)
                df_clean = clean(color, df)
                write_local(color, df_clean, dataset_file)
            else:
                print(f"Skipping invalid URL: {dataset_url}")
