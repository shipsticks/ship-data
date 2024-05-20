import os
import logging

# from google.oauth2 import service_account
from google.cloud import bigquery
# from google.cloud import secretmanager

GCP_PROJECT = "gse-dw-prod"

def run_sql_file(sql_filepath: str) -> None:
    logging.info(f"Starting sql file: {sql_filepath}")
    print(f"Starting sql file: {sql_filepath}")
    working_dir = os.getcwd()
    full_path = working_dir + f"{sql_filepath}"
    client = bigquery.Client(project=GCP_PROJECT)
    with open(full_path, "r") as f:
        sql = f.read()
        logging.info(f"reading query: {full_path}")
    # run sql job
    bigquery_job = client.query(sql)
    logging.info(f"bigquery_job: {bigquery_job}")


if __name__ == "__main__":
    run_sql_file("/sql/test_file.sql")
