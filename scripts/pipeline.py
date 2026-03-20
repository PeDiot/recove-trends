from typing import List
from pydantic import BaseModel

import os, time, requests, json

from dagster import Config, get_dagster_logger, job, op, ScheduleDefinition

from google.cloud import bigquery
from google.oauth2 import service_account

from slack_sdk import WebClient


DBT_ACCOUNT_ID = os.getenv("DBT_ACCOUNT_ID")
DBT_JOB_ID = os.getenv("DBT_JOB_ID")
DBT_API_TOKEN = os.getenv("DBT_API_TOKEN")

SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN")
SLACK_CHANNEL = os.getenv("SLACK_CHANNEL")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_DATASET_ID = os.getenv("GCP_DATASET_ID")
GCP_CREDENTIALS_JSON = os.getenv("GCP_CREDENTIALS_JSON")


class TrendsConfig(Config):
    ngrams_n: int = 2
    top_k: int = 50
    lookback_hours: int = 24


class Trend(BaseModel):
    ngram: str
    num_search_query_events: int
    num_click_out_events: int
    num_save_events: int
    num_converting_users: int
    engagement_score: int

    def to_message(self, rank: int) -> str:
        return (
            f"{rank}. *{self.ngram}* — Score: {self.engagement_score} "
            f"({self.num_search_query_events} searches; "
            f"{self.num_click_out_events} click-outs; "
            f"{self.num_save_events} saves; "
            f"{self.num_converting_users} converting users)\n"
        )


def create_bigquery_client() -> bigquery.Client:
    creds_dict = json.loads(GCP_CREDENTIALS_JSON)
    credentials = service_account.Credentials.from_service_account_info(creds_dict)

    return bigquery.Client(
        project=GCP_PROJECT_ID,
        credentials=credentials,
    )


@op
def run_dbt_cloud_job() -> bool:
    logger = get_dagster_logger()
    headers = {"Authorization": f"Token {DBT_API_TOKEN}"}

    trigger_url = f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/jobs/{DBT_JOB_ID}/run/"
    response = requests.post(
        trigger_url, headers=headers, json={"cause": "Triggered by Dagster"}
    )
    logger.info(response.text)
    response.raise_for_status()
    run_id = response.json()["data"]["id"]

    logger.info(f"Started dbt run {run_id}. Waiting for completion...")

    status_url = (
        f"https://cloud.getdbt.com/api/v2/accounts/{DBT_ACCOUNT_ID}/runs/{run_id}/"
    )
    while True:
        status_res = requests.get(status_url, headers=headers).json()
        status = status_res["data"]["status"]

        if status == 10:
            logger.info("dbt Job Succeeded!")
            return True
        elif status == 20:
            raise Exception(f"dbt Job Failed! Check dbt Cloud logs for run {run_id}")

        time.sleep(15)


@op
def fetch_top_trends(config: TrendsConfig, dbt_success: bool) -> List[Trend]:
    if not dbt_success:
        raise Exception(
            "dbt job did not succeed; skipping BigQuery query and Slack notification."
        )

    client = create_bigquery_client()

    table_name = (
        f"trends_{config.ngrams_n}grams_{config.lookback_hours}h_top{config.top_k}"
    )

    query = f"""
    SELECT
        ngram,
        num_search_query_events,
        num_click_out_events,
        num_save_events,
        num_converting_users,
        engagement_score
    FROM `{GCP_PROJECT_ID}.{GCP_DATASET_ID}.{table_name}`
    ORDER BY engagement_score DESC
    LIMIT {config.top_k}
    """

    results = client.query(query).result()

    trends = []
    for row in results:
        trend = Trend(
            ngram=row.ngram,
            num_search_query_events=row.num_search_query_events,
            num_click_out_events=row.num_click_out_events,
            num_save_events=row.num_save_events,
            num_converting_users=row.num_converting_users,
            engagement_score=row.engagement_score,
        )
        trends.append(trend)

    return trends


@op
def send_slack_alert(config: TrendsConfig, trends: List[Trend]):
    client = WebClient(token=SLACK_BOT_TOKEN)

    message = f"📈 *Daily Recove Search Trends (Top {config.top_k} - Last {config.lookback_hours}h)*\n\n"
    for i, trend in enumerate(trends, 1):
        message += trend.to_message(i)

    client.chat_postMessage(channel=SLACK_CHANNEL, text=message)
    get_dagster_logger().info("Slack alert sent successfully!")


@job
def daily_trends_pipeline():
    success = run_dbt_cloud_job()
    trends = fetch_top_trends(success)
    send_slack_alert(trends)


daily_schedule = ScheduleDefinition(
    job=daily_trends_pipeline, cron_schedule="0 0 * * *"
)
