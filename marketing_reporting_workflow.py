import pendulum
from datetime import timedelta

import requests
from google.cloud import bigquery

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

GCP_CONN_ID = "gcp_connection"
PROJECT_ID = "marketing-analytics-project"
DAG_NAME = "marketing_data_pipeline"

DEFAULT_ARGS = {
"owner": "...",
"depends_on_past": False,
"start_date": pendulum.datetime(2024, 1, 1, tz="Asia/Ho_Chi_Minh"),
}

GOOGLE_CHAT_WEBHOOK = "YOUR_GOOGLE_CHAT_WEBHOOK"

def send_daily_report():

```
client = bigquery.Client(project=PROJECT_ID)

query = """
SELECT
    channel,
    SUM(cost) AS total_cost
FROM `marketing_dataset.daily_campaign_performance`
WHERE day = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY channel
"""

rows = list(client.query(query).result())

total_cost = sum(r["total_cost"] for r in rows)

message = [f"Daily Cost Report: {total_cost:,.0f}"]

for row in rows:
    message.append(
        f"- {row['channel']}: {row['total_cost']:,.0f}"
    )

requests.post(
    GOOGLE_CHAT_WEBHOOK,
    json={"text": "\n".join(message)}
)
```

with DAG(
dag_id=DAG_NAME,
default_args=DEFAULT_ARGS,
schedule_interval="30 8 * * *",
catchup=False
) as dag:

```
with TaskGroup(group_id="out_app"):

    extract_ads_data = ...
    transform_ads_data = ...
    aggregate_performance = ...

with TaskGroup(group_id="in_app"):

    user_profile = ...
    attribution = ...
    campaign_analysis = ...

notify_daily_report = PythonOperator(
    task_id="notify_daily_report",
    python_callable=send_daily_report,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

out_app >> in_app >> notify_daily_report
```
