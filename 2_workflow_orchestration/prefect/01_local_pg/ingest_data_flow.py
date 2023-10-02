#!/usr/bin/env python
# coding: utf-8
import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector


@task(
    log_prints=True,
    tags=["extract"],
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def extract_data(url: str) -> pd.DataFrame:
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file

    if url.endswith(".csv.gz"):
        csv_name = "C:/Users/Anton/Desktop/DE2/data/multistore/2019-Oct.csv"
    else:
        csv_name = "output.csv"

    # os.system(f"wget {url} -O {csv_name}")

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.event_time = pd.to_datetime(df.event_time)

    return df


@task(log_prints=True)
def transform_data(df: pd.DataFrame) -> pd.DataFrame:
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


@task(log_prints=True, retries=3)
def load_data(table_name, df):
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists="replace")
        df.to_sql(name=table_name, con=engine, if_exists="append")


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
    print(f"Logging Subflow for: {table_name}")


@flow(name="Ingest Data")
def main_flow(table_name: str = "multi_store_oct"):
    csv_url = "https://data.rees46.com/datasets/marketplace/2019-Oct.csv.gz"
    log_subflow(table_name)
    raw_data = extract_data(csv_url)
    data = transform_data(raw_data)
    load_data(table_name, data)


if __name__ == "__main__":
    main_flow(table_name="multi_store_oct")
