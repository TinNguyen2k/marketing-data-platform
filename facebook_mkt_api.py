from facebook_business.api import FacebookAdsApi
from facebook_business.adobjects.adaccount import AdAccount
from google.cloud import bigquery
import pandas as pd, time
from datetime import datetime, timedelta

# =============== CONFIG ===============
access_token = "access_token"

ad_account_ids = [
    'act_xxx', 'act_xxx'
]

# =============== BIGQUERY CONFIG ===============
PROJECT_ID       = "project_id"
DATASET_ID       = "REPORT"
DEST_TABLE       = "MEDIA_FACT_FB_AD_REPORT"
BILLING_PROJECT  = "billing_project"

client = bigquery.Client(project=BILLING_PROJECT)
table_id = f"{PROJECT_ID}.{DATASET_ID}.{DEST_TABLE}"

# =============== DETERMINE DATE RANGE ===============
try:
    query = f"""
        SELECT MAX(CAST(date_stop AS DATE)) AS max_date
        FROM `{table_id}`
    """
    result = client.query(query).result()
    row = list(result)[0]
    max_date = row.max_date
except Exception as e:
    print(f"Table not found or cannot query ({e}), starting from default date.")
    max_date = None

if max_date is None:
    start_date = datetime(2025, 10, 6)
else:
    start_date = datetime.combine(max_date + timedelta(days=1), datetime.min.time())

end_date = datetime.combine(datetime.now() - timedelta(days=1), datetime.min.time())

if start_date > end_date:
    print("✅ Table is already up to date. No new data to fetch.")
    exit()

print(f"📅 Fetching data from {start_date.date()} → {end_date.date()}")

# =============== FACEBOOK FETCH CONFIG ===============
fields = [
    'date_start','date_stop','ad_id','campaign_id','adset_id',
    'spend','impressions','clicks','reach',
    'account_id','account_currency','cpc','ctr','cpm'
]

FacebookAdsApi.init(access_token=access_token)

parameters = {
    "level": "ad",
    "time_increment": 1,
    "breakdowns": ["publisher_platform", "platform_position"],  # ✅ granular theo platform
}

# =============== FETCH FUNCTION ===============
def get_insights(acc_id, since, until):
    ad_account = AdAccount(acc_id)
    params = parameters.copy()
    params["limit"] = 500
    params["time_range"] = {
        "since": since.strftime("%Y-%m-%d"),
        "until": until.strftime("%Y-%m-%d"),
    }

    try:
        cursor = ad_account.get_insights(fields=fields, params=params)
        return [dict(x) for x in cursor]
    except Exception as e:
        print(f"❌ Error fetching {acc_id}: {e}")
        return []

# =============== MAIN FETCH LOOP ===============
all_rows = []
curr_date = start_date

while curr_date <= end_date:
    for idx, acc_id in enumerate(ad_account_ids, start=1):
        print(f"\nFetching {idx}/{len(ad_account_ids)}: {acc_id} | {curr_date.date()}")
        data = get_insights(acc_id, curr_date, curr_date)
        all_rows.extend(data)
        print(f"✅ Done {len(data)} rows")
        time.sleep(2)
    curr_date += timedelta(days=1)

df = pd.DataFrame(all_rows)
print("\n🎯 Fetch complete! Total rows:", len(df))

# =============== UPLOAD TO BIGQUERY ===============
if df.empty:
    print("⚠️ No data fetched — skip BigQuery upload.")
else:
    df["loaded_at"] = datetime.utcnow()

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        autodetect=True
    )

    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()

    print(f"✅ Inserted {len(df)} rows vào {table_id}, job chạy bằng project {BILLING_PROJECT}")
