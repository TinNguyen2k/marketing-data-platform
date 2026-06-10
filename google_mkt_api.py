import requests
import json
import pandas as pd
from pandas import json_normalize
from datetime import datetime, timedelta
from google.cloud import bigquery

# ================== CONFIG ==================
client_id = "xxx"
client_secret = "xxx"
refresh_token = "xxx"
developer_token = "xxx"
login_customer_id = "xxx"

customer_ids = [
    "xxx","xxx"
]

PROJECT_ID = "project_id"
DATASET_ID = "REPORT"
DEST_TABLE = "MEDIA_FACT_GG_AD_REPORT"
BILLING_PROJECT = "billing_project"

# ================== AUTH ==================
def get_access_token():
    res = requests.post("https://oauth2.googleapis.com/token", data={
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
        "grant_type": "refresh_token"
    })
    res.raise_for_status()
    return res.json()["access_token"]

# ================== FETCH ==================
def fetch_data(customer_ids, access_token, start_date, end_date):
    all_data = []
    for cid in customer_ids:
        print(f"▶️ Fetching {cid} ({start_date} → {end_date})")
        headers = {
            "Authorization": f"Bearer {access_token}",
            "developer-token": developer_token,
            "login-customer-id": login_customer_id,
            "Content-Type": "application/json"
        }
        body = {
            "query": f"""
                SELECT
                    segments.date,
                    ad_group_ad.ad.id,
                    ad_group_ad.ad_group,
                    ad_group.campaign,
                    metrics.conversions,
                    metrics.clicks,
                    metrics.impressions,
                    metrics.engagements,
                    metrics.interactions,
                    metrics.cost_micros,
                    metrics.video_views,
                    metrics.video_quartile_p25_rate,
                    metrics.video_quartile_p50_rate,
                    metrics.video_quartile_p75_rate,
                    metrics.video_quartile_p100_rate,
                    metrics.video_view_rate,
                    customer.resource_name,
                    customer.currency_code
                FROM ad_group_ad
                WHERE segments.date BETWEEN '{start_date}' AND '{end_date}'
            """
        }
        try:
            res = requests.post(
                f"https://googleads.googleapis.com/v20/customers/{cid}/googleAds:search",
                headers=headers,
                data=json.dumps(body)
            )
            res.raise_for_status()
            results = res.json().get("results", [])
            df = json_normalize(results)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            print(f"❌ Failed for {cid}: {e}")

    return pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()

# ================== MAIN ==================
if __name__ == "__main__":
    access_token = get_access_token()

    FULL_REFRESH = False

    today = datetime.utcnow().date()
    end_date = today   # đổi từ D-1 thành D0

    client = bigquery.Client(project=BILLING_PROJECT)
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{DEST_TABLE}"

    query = f"SELECT MAX(date) as max_date FROM `{table_id}`"
    result = client.query(query).result()
    rows = list(result)
    max_date = rows[0].max_date if rows and rows[0].max_date else None

    if FULL_REFRESH or not max_date:
        start_date = datetime(2024, 1, 1).date()
    else:
        start_date = max_date - timedelta(days=9)

    print(f"⏳ Fetching data from {start_date} → {end_date}")

    df = fetch_data(customer_ids, access_token, start_date, end_date)

    if not df.empty:
        df_final = pd.DataFrame({
            "account_id": df["customer.resourceName"].str.split("/").str[1].astype("int64"),
            "adgroup_id": df["adGroupAd.adGroup"].str.split("/").str[3],
            "ad_id": df["adGroupAd.ad.id"].astype("int64"),
            "campaign_id": df["adGroup.campaign"].str.split("/").str[3].astype("int64"),
            "clicks": df["metrics.clicks"].astype("int64"),
            "conversions": df["metrics.conversions"].astype(float),
            "cost_micros": df["metrics.costMicros"].astype(float),
            "engagements": df["metrics.engagements"].astype("int64"),
            "impressions": df["metrics.impressions"].astype("int64"),
            "interactions": df["metrics.interactions"].astype("int64"),
            "video_views": df["metrics.videoViews"].astype("int64"),
            "video_q25_rate": df["metrics.videoQuartileP25Rate"].astype(float),
            "video_q50_rate": df["metrics.videoQuartileP50Rate"].astype(float),
            "video_q75_rate": df["metrics.videoQuartileP75Rate"].astype(float),
            "video_q100_rate": df["metrics.videoQuartileP100Rate"].astype(float),
            "video_view_rate": pd.to_numeric(df.get("metrics.videoViewRate"), errors="coerce"),
            "currency": df["customer.currencyCode"],
            "date": pd.to_datetime(df["segments.date"], errors="coerce").dt.date,
            "fetch_datetime": datetime.utcnow()
        })

        # ================== DELETE OLD RANGE ==================
        delete_sql = f"""
            DELETE FROM `{table_id}`
            WHERE date BETWEEN '{start_date}' AND '{end_date}'
        """
        client.query(delete_sql).result()
        print(f"🗑️ Deleted old rows {start_date} → {end_date} in {table_id}")

        # ================== LOAD TO BIGQUERY ==================
        job = client.load_table_from_dataframe(
            df_final,
            table_id,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        )
        job.result()

        print(f"✅ Inserted {len(df_final)} rows vào {table_id}, job chạy bằng project {BILLING_PROJECT}")

    else:
        print("⚠️ Không có data fetch được.")
