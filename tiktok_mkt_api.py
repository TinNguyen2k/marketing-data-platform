from __future__ import print_function
import time
import business_api_client
from business_api_client.rest import ApiException
from datetime import datetime, timedelta
from business_api_client.tiktok_business.tiktok_code import NumericErrorCodes
import pandas as pd
from google.cloud import bigquery

# ================= CONFIG =================
target_project = "project_id"
billing_project = "billing_project"
dataset_id = "REPORT"
table_id = "MEDIA_FACT_TIKTOK_AD_REPORT"

access_token = "access_token"

FULL_REFRESH = False

ad_account_ids = [
    "xxx",
    "xxx"
]

# Metrics & dimensions
dimensions = ["ad_id", "stat_time_day"]
metrics = [
    "spend", "impressions", "clicks", "reach", "currency", "engagements",
    "video_play_actions", "video_watched_2s", "video_watched_6s", "paid_engaged_view_15s",
    "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
    "average_video_play", "frequency", "cpm", "cpc", "ctr", "conversion", "app_install",
    "advertiser_id", "advertiser_name", "campaign_id", "campaign_name", "objective_type",
    "adgroup_id", "adgroup_name", "ad_name", "ad_id"
]

# ================= DATE RANGE HANDLER =================
def get_date_range():
    client = bigquery.Client(project=billing_project)
    table_ref = f"{target_project}.{dataset_id}.{table_id}"

    if FULL_REFRESH:
        start_date = datetime(year=2025, month=1, day=1)
    else:
        query = f"""
            SELECT MAX(stat_time_day) AS max_date
            FROM `{table_ref}`
        """
        result = client.query(query).result()
        row = list(result)
        max_date = row[0].max_date if row and row[0].max_date else None

        if max_date:
            if isinstance(max_date, str):
                try:
                    max_date = datetime.strptime(max_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    max_date = datetime.strptime(max_date, "%Y-%m-%d")
            elif not isinstance(max_date, datetime):
                max_date = datetime.combine(max_date, datetime.min.time())

            # Lùi 10 ngày trước max_date
            start_date = max_date - timedelta(days=10)
        else:
            start_date = datetime(year=2025, month=1, day=1)

    # End date là hôm nay (thay vì D-1)
    end_date = datetime.now()

    if start_date > end_date:
        print("⚠️ No new data to load (already up-to-date).")
        return None, None

    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")

# ================= FETCH DATA =================
def fetch_tiktok_data(ad_account_ids, start_date, end_date):
    api_instance = business_api_client.ReportingApi()
    all_results = []

    for advertiser_id in ad_account_ids:
        print(f"▶️ Fetching data for advertiser_id = {advertiser_id}, {start_date} → {end_date}")

        try:
            api_response = api_instance.report_integrated_get(
                advertiser_id=advertiser_id,
                report_type="BASIC",
                dimensions=dimensions,
                access_token=access_token,
                data_level="AUCTION_AD",
                metrics=metrics,
                start_date=start_date,
                end_date=end_date,
                query_lifetime="false",
                page=1,
                page_size=1000,
                query_mode="CHUNK",
                async_req=True
            )

            while True:
                request = api_response.get()
                request_dict = request.to_dict()
                code = request_dict.get("code")
                message = request_dict.get("message")

                if code == NumericErrorCodes.ERROR_CODE_OK:
                    print(f"✅ Job run completed for {advertiser_id}")

                    raw_data = request_dict.get("data", {}).get("list", [])
                    flat_data = []

                    for item in raw_data:
                        row = {}
                        row.update(item.get("dimensions", {}))
                        row.update(item.get("metrics", {}))
                        flat_data.append(row)

                    df = pd.DataFrame(flat_data)

                    for col in metrics + dimensions:
                        if col not in df.columns:
                            df[col] = None

                    df = df[["stat_time_day"] + metrics]
                    df["advertiser_id_source"] = advertiser_id
                    all_results.append(df)
                    break

                else:
                    print(f"⚠️ Error {code}: {message}")
                    break

                time.sleep(5)

        except ApiException as e:
            print("❌ Exception when calling ReportingApi->report_integrated_get: %s\n" % e)

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df = final_df.sort_values(
            ["stat_time_day", "advertiser_id_source"],
            ascending=[False, True]
        )
        print("📊 Final merged data:", final_df.shape)
        return final_df

    return pd.DataFrame()

# ================= LOAD TO BIGQUERY =================
def load_to_bigquery(df, start_date, end_date):
    if df is None:
        print("⚠️ Dataframe is None. Nothing to load.")
        return

    if df.empty:
        print("⚠️ No data to load to BigQuery")
        return

    client = bigquery.Client(project=billing_project)
    table_ref = f"{target_project}.{dataset_id}.{table_id}"

    # FULL refresh -> truncate whole table
    if FULL_REFRESH:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        print("🔄 Running FULL REFRESH: old data will be replaced.")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"🚀 Loaded {len(df)} rows to {table_ref}")
        return

    # ========== incremental with safe delete ==========
    # use robust DATE(substr(cast(...))) so it matches DATE/DATETIME/TIMESTAMP/STRING
    date_expr = "DATE(SUBSTR(CAST(stat_time_day AS STRING), 1, 10))"

    # 1) Count rows that WOULD be deleted
    count_sql = f"""
        SELECT COUNT(1) AS cnt
        FROM `{table_ref}`
        WHERE {date_expr} BETWEEN DATE('{start_date}') AND DATE('{end_date}')
    """
    try:
        count_rows = list(client.query(count_sql).result())
        pre_cnt = int(count_rows[0].cnt) if count_rows else 0
    except Exception as e:
        print("❌ Failed to get pre-delete count:", e)
        pre_cnt = None

    print(f"ℹ️ Rows in BQ within {start_date} → {end_date}: {pre_cnt}")

    # 2) Delete only if there are rows (and pre_cnt is not None)
    if pre_cnt:
        delete_sql = f"""
            DELETE FROM `{table_ref}`
            WHERE {date_expr} BETWEEN DATE('{start_date}') AND DATE('{end_date}')
        """
        try:
            client.query(delete_sql).result()
            print(f"🗑️ Deleted {pre_cnt} rows from {start_date} → {end_date}")
        except Exception as e:
            print("❌ Delete failed:", e)
            # fail-safe: abort loading to avoid duplicates
            return
    else:
        print("ℹ️ No existing rows to delete in that range. Proceeding to load new data.")

    # 3) Load (append) the fetched data
    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"🚀 Loaded {len(df)} rows to {table_ref}")
    except Exception as e:
        print("❌ Load to BigQuery failed:", e)

# ================= RUN =================
if __name__ == "__main__":
    start_date, end_date = get_date_range()

    if not start_date or not end_date:
        print("✅ Data already up-to-date. Nothing to fetch.")
    else:
        print(f"⏱ Date range: {start_date} → {end_date}")
        final_df = fetch_tiktok_data(ad_account_ids, start_date, end_date)
        load_to_bigquery(final_df, start_date, end_date)
