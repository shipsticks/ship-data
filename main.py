import os
from loguru import logger
from google.cloud import bigquery

GCP_PROJECT = "gse-dw-prod"
bq_client = bigquery.Client(project=GCP_PROJECT)

# @logger.catch
def run_sql(sql_file: str = None, sql: str = None) -> None:
    
    working_dir = os.getcwd()
    full_path = working_dir + f"/sql/{sql_file}"
    if sql_file is not None:
        with open(full_path, "r") as f:
            sql = f.read()
    elif sql is not None:
        sql_file = sql
    elif sql is None:
        logger.exception("both 'sql_file' and 'sql' are missing")
        return

    try:
        # run sql job
        logger.info(f"Executing: {sql_file}")
        bigquery_job = bq_client.query(sql)
        # get results
        _ = bigquery_job.result()
        logger.info(f"Completed: {sql_file} - job-id: {bigquery_job}")
    except Exception:
        logger.exception(f"Error: {sql_file}", exc_info=True)


def discovery_env() -> list[str]:
    local_files = os.listdir(os.getcwd())
    logger.info(f"Discovered files: {local_files}")


def run_workflow() -> None:
    discovery_env()
    run_sql(sql="select 1/0 as test")
    run_sql("rudderstack_events.sql")
    run_sql("ad_spend.sql")
    run_sql("prospect_attrabution.sql")
    run_sql("utm_campaigns.sql")
    run_sql("users.sql")


if __name__ == "__main__":
    run_workflow()
