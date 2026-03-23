from typing import List, Dict
from pydantic import BaseModel

import time, requests, json
from decouple import config

from dagster import Config, get_dagster_logger, job, op, ScheduleDefinition

from google.cloud import bigquery
from google.oauth2 import service_account

from slack_sdk import WebClient


DBT_API_URL = "https://ar616.us1.dbt.com/api/v2/accounts"
DBT_ACCOUNT_ID = config("DBT_ACCOUNT_ID")
DBT_JOB_ID = config("DBT_JOB_ID")
DBT_API_TOKEN = config("DBT_API_TOKEN")
DBT_JOB_STATUS_SUCCESS = 10
DBT_JOB_STATUS_FAILED = 20

SLACK_BOT_TOKEN = config("SLACK_BOT_TOKEN")
SLACK_CHANNEL = config("SLACK_CHANNEL")

GCP_PROJECT_ID = config("GCP_PROJECT_ID")
GCP_DATASET_ID = config("GCP_DATASET_ID")
GCP_CREDENTIALS_JSON = config("GCP_CREDENTIALS_JSON")


class TrendsConfig(Config):
    ngrams_n: int = 2
    lookback_hours: int = 24
    top_k: int = 100
    top_k_display: int = 10


class Trend(BaseModel):
    ngram: str
    num_search_query_events: int
    num_click_out_events: int
    num_save_events: int
    num_converting_users: int
    engagement_score: float
    normalized_score: float

    @classmethod
    def from_row(cls, row: bigquery.Row) -> "Trend":
        return cls(
            ngram=row.ngram,
            num_search_query_events=row.num_search_query_events,
            num_click_out_events=row.num_click_out_events,
            num_save_events=row.num_save_events,
            num_converting_users=row.num_converting_users,
            engagement_score=row.engagement_score,
            normalized_score=row.normalized_score,
        )

    def to_message(self, rank: int) -> str:
        return (
            f"{rank}. *{self.ngram}*\n"
            f"    • Engagement: *{self.normalized_score:.2%}*\n"
            f"    • 🔎 {self.num_search_query_events:,} | "
            f"🔗 {self.num_click_out_events:,} | "
            f"🩶 {self.num_save_events:,} | "
            f"👥 {self.num_converting_users:,}\n\n"
        )


def create_dbt_payload(config: TrendsConfig) -> Dict:
    dbt_vars = {
        "ngrams_n": config.ngrams_n,
        "lookback_hours": config.lookback_hours,
        "top_k": config.top_k,
    }

    vars_str = json.dumps(dbt_vars)
    dbt_command = f"dbt build --select dynamic_trends_ngrams --vars '{vars_str}'"

    return {"cause": "Triggered by Dagster", "steps_override": [dbt_command]}


def initialize_bq_client() -> bigquery.Client:
    creds_dict = json.loads(GCP_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)

    return bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )


def create_bq_retrieval_query(config: TrendsConfig) -> str:
    table_name = (
        f"trends_{config.ngrams_n}grams_{config.lookback_hours}h_top{config.top_k}"
    )

    return f"""
SELECT
ngram,
num_search_query_events,
num_click_out_events,
num_save_events,
num_converting_users,
engagement_score,
normalized_score
FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{table_name}`
ORDER BY engagement_score DESC
LIMIT {config.top_k_display};
    """


def format_lookback_hours(lookback_hours: int) -> str:
    if lookback_hours > 24:
        days = lookback_hours // 24
        return f"{days} days"

    return f"{lookback_hours} hours"


@op
def trigger_dbt_job(config: TrendsConfig) -> int:
    logger = get_dagster_logger()

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {DBT_API_TOKEN}",
    }

    payload = create_dbt_payload(config)
    url = f"{DBT_API_URL}/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    response = requests.post(url, headers=headers, json=payload)

    logger.info(response.text)
    response.raise_for_status()

    run_id = response.json()["data"]["id"]
    logger.info(f"Successfully triggered dbt run {run_id}.")

    return run_id


@op
def wait_for_dbt_job(run_id: int) -> bool:
    logger = get_dagster_logger()

    headers = {
        "Accept": "application/json",
        "Authorization": f"Bearer {DBT_API_TOKEN}",
    }

    logger.info(f"Waiting for completion of dbt run {run_id}...")
    url = f"{DBT_API_URL}/{DBT_ACCOUNT_ID}/runs/{run_id}/"

    while True:
        status_res = requests.get(url, headers=headers).json()
        status = status_res["data"]["status"]

        if status == DBT_JOB_STATUS_SUCCESS:
            logger.info("dbt Job Succeeded!")
            return True
        elif status == DBT_JOB_STATUS_FAILED:
            raise Exception(f"dbt Job Failed! Check dbt Cloud logs for run {run_id}")

        time.sleep(15)


@op
def fetch_top_trends(config: TrendsConfig, dbt_success: bool) -> List[Trend]:
    if not dbt_success:
        raise Exception(
            "dbt job did not succeed; skipping BigQuery query and Slack notification."
        )

    client = initialize_bq_client()
    query = create_bq_retrieval_query(config)
    rows = client.query(query).result()

    return [Trend.from_row(row) for row in rows]


@op
def send_slack_alert(config: TrendsConfig, trends: List[Trend]):
    client = WebClient(token=SLACK_BOT_TOKEN)
    lookback_display = format_lookback_hours(config.lookback_hours)

    message = (
        f"💅🏻 *Top {config.top_k_display} Recove Trends "
        f"over the last {lookback_display}*\n\n"
    )

    for i, trend in enumerate(trends, 1):
        message += trend.to_message(i)

    client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
    get_dagster_logger().info("Slack alert sent successfully!")


@job
def daily_trends_pipeline():
    run_id = trigger_dbt_job()
    success = wait_for_dbt_job(run_id=run_id)
    trends = fetch_top_trends(dbt_success=success)
    send_slack_alert(trends=trends)


daily_schedule = ScheduleDefinition(
    job=daily_trends_pipeline, cron_schedule="0 0 * * *"
)
