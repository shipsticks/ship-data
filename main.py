import os
from loguru import logger
from google.cloud import bigquery

GCP_PROJECT = "gse-dw-prod"


@logger.catch
def run_sql_file(sql_file: str) -> None:
    logger.info(f"Executing sql file: {sql_file}")
    working_dir = os.getcwd()
    full_path = working_dir + f"/sql/{sql_file}"
    client = bigquery.Client(project=GCP_PROJECT)
    with open(full_path, "r") as f:
        sql = f.read()
    # run sql job
    bigquery_job = client.query(sql)
    # get results
    _ = bigquery_job.result()
    logger.info(f"Completed job: {bigquery_job}")


def discovery_env() -> list[str]:
    local_files = os.listdir(os.getcwd())
    logger.info(f"Discovered files: {local_files}")


@logger.catch
def test_fail() -> None:
    client = bigquery.Client(project=GCP_PROJECT)
    logger.info("starting job: fail test")
    bigquery_job = client.query("select 1/0 as test")
    _ = bigquery_job.result()
    logger.info(f"Completed job: {bigquery_job}")


def run_workflow() -> None:
    discovery_env()
    run_sql_file("rudderstack_events.sql")
    run_sql_file("ad_spend.sql")
    run_sql_file("prospect_attrabution.sql")
    run_sql_file("utm_campaigns.sql")
    run_sql_file("users.sql")


if __name__ == "__main__":
    run_workflow()
