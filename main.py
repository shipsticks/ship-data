import os
from loguru import logger
from google.cloud import bigquery

GCP_PROJECT = "gse-dw-prod"
bq_client = bigquery.Client(project=GCP_PROJECT)


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
        bigquery_job = bq_client.query(sql)
        logger.info(f"Executing: {sql_file} - job-id: {bigquery_job}")
        # get results
        _ = bigquery_job.result()
    except Exception:
        logger.exception(f"Error: {sql_file}", exc_info=True)


def template_sql(sql_file: str, params: list[dict]) -> None:
    working_dir = os.getcwd()
    full_path = working_dir + f"/sql/{sql_file}"
    with open(full_path, "r") as f:
        sql_template = f.read()

    for param in params:
        sql = sql_template.format(**param)
        try:
            # run sql job
            bigquery_job = bq_client.query(sql)
            logger.info(f"Executing: {sql_file}, params: {param} - job-id: {bigquery_job}")
            # get results
            _ = bigquery_job.result()
        except Exception:
            logger.exception(f"Error: {sql_file}", exc_info=True)


def finsum_metrics_union() -> None:
    sql = '''
    drop table if exists dp_staging_finsum.finsum_metrics;
    create table if not exists dp_staging_finsum.finsum_metrics 
    as
    select * from dp_staging_finsum.metrics_transaction_financial_date
    union all
    select * from dp_staging_finsum.metrics_shipment_created_date
    union all
    select * from dp_staging_finsum.metrics_shipment_actual_delivery_date
    '''
    run_sql(sql=sql)


def discovery_env() -> list[str]:
    local_files = os.listdir(os.getcwd())       ∫b
    logger.info(f"Discovered files: {local_files}")
    return local_files


def run_workflow() -> None:
    discovery_env()
    run_sql("rudderstack_events.sql")
    run_sql("ad_spend.sql")
    run_sql("prospect_attrabution.sql")
    run_sql("utm_campaigns.sql")
    run_sql("paid_campaigns.sql")
    run_sql("users.sql")
    run_sql("finsum.sql")
    run_sql("finsum_metrics.sql")
    template_sql(
        sql_file="template_finsum_metrics.sql",
        params=[
            {"report_date": "transaction_financial_date"},
            {"report_date": "shipment_created_date"},
            {"report_date": "shipment_actual_delivery_date"},
        ],
    )
    finsum_metrics_union()


if __name__ == "__main__":
    run_workflow()
