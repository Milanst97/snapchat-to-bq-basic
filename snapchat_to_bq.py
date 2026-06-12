import os
import json
import time
import random
import datetime as dt
from zoneinfo import ZoneInfo

import requests
from google.cloud import bigquery


SNAP_CLIENT_ID = os.environ["SNAP_CLIENT_ID"]
SNAP_CLIENT_SECRET = os.environ["SNAP_CLIENT_SECRET"]
SNAP_REFRESH_TOKEN = os.environ["SNAP_REFRESH_TOKEN"]
SNAP_AD_ACCOUNT_IDS = os.environ["SNAP_AD_ACCOUNT_IDS"]

SWIPE_UP_ATTRIBUTION_WINDOW = os.getenv("SWIPE_UP_ATTRIBUTION_WINDOW", "28_DAY")
VIEW_ATTRIBUTION_WINDOW = os.getenv("VIEW_ATTRIBUTION_WINDOW", "1_DAY")
REPORTING_TZ = os.getenv("REPORTING_TZ", "Etc/GMT-1")
LOOKBACK_DAYS = int(os.getenv("LOOKBACK_DAYS", "28"))

START_DATE = os.getenv("START_DATE", "").strip()
END_DATE = os.getenv("END_DATE", "").strip()

CHUNK_DAYS = int(os.getenv("CHUNK_DAYS", "30"))
REQUEST_DELAY = float(os.getenv("REQUEST_DELAY", "1"))

BQ_PROJECT = os.environ["BQ_PROJECT"]
BQ_DATASET = os.environ["BQ_DATASET"]
AD_TABLE = os.getenv("BQ_AD_TABLE", "snapchat_ads")
CONVERSIONS_TABLE = os.getenv("BQ_CONVERSIONS_TABLE", "snapchat_ads_conversions")

SOURCE = "snapchat"
API_BASE = "https://adsapi.snapchat.com/v1"
TOKEN_URL = "https://accounts.snapchat.com/login/oauth2/access_token"

CONVERSION_FIELDS = [
    ("conversion_view_content", "view_content"),
    ("conversion_add_cart", "add_cart"),
    ("conversion_start_checkout", "start_checkout"),
    ("conversion_purchases", "purchase"),
    ("conversion_subscribe", "subscribe"),
    ("conversion_reserve", "reserve"),
    ("custom_event_1", "custom_event_1"),
]

BASE_FIELDS = ["impressions", "swipes", "spend"]
FIELDS_PARAM = ",".join(
    BASE_FIELDS
    + [f for f, _ in CONVERSION_FIELDS]
    + [f"{f}_value" for f, _ in CONVERSION_FIELDS]
)


def parse_account_ids(v):
    return [a.strip() for a in (v or "").split(",") if a.strip()]


def safe_int(x):
    try:
        return int(float(x))
    except Exception:
        return None


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return None


def safe_str(x):
    if x is None:
        return None
    return str(x)


def micros_to_units(x):
    v = safe_float(x)
    if v is None:
        return None
    return v / 1_000_000


def get_date_range():
    if START_DATE and END_DATE:
        return dt.date.fromisoformat(START_DATE), dt.date.fromisoformat(END_DATE)
    tz = ZoneInfo(REPORTING_TZ)
    today = dt.datetime.now(tz).date()
    until = today - dt.timedelta(days=1)
    since = until - dt.timedelta(days=LOOKBACK_DAYS - 1)
    return since, until


TOKEN = {"access_token": None, "expires_at": 0}


def get_access_token(force=False):
    if not force and TOKEN["access_token"] and time.time() < TOKEN["expires_at"] - 120:
        return TOKEN["access_token"]
    data = {
        "grant_type": "refresh_token",
        "client_id": SNAP_CLIENT_ID,
        "client_secret": SNAP_CLIENT_SECRET,
        "refresh_token": SNAP_REFRESH_TOKEN,
    }
    last_err = None
    for attempt in range(1, 6):
        try:
            r = requests.post(TOKEN_URL, data=data, timeout=60)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(5 * attempt)
                continue
            r.raise_for_status()
            body = r.json()
            TOKEN["access_token"] = body["access_token"]
            TOKEN["expires_at"] = time.time() + int(body.get("expires_in", 1800))
            return TOKEN["access_token"]
        except Exception as e:
            last_err = e
            time.sleep(5 * attempt)
    raise RuntimeError(f"snapchat token refresh failed: {last_err}")


def request_with_retries(url, params, max_attempts=10):
    last_r = None
    last_body = None

    for attempt in range(1, max_attempts + 1):
        headers = {"Authorization": f"Bearer {get_access_token()}"}
        r = requests.get(url, params=params, headers=headers, timeout=90)
        last_r = r

        if r.status_code == 401:
            get_access_token(force=True)
            continue

        if r.status_code in (429, 500, 502, 503, 504):
            time.sleep(min(300, 30 * attempt) + random.random())
            continue

        try:
            body = r.json()
        except Exception:
            body = {"non_json": r.text[:2000]}
        last_body = body

        return r, body

    return last_r, (last_body or {"non_json": getattr(last_r, "text", "")[:2000]})


def snap_fetch_account(account_id):
    url = f"{API_BASE}/adaccounts/{account_id}"
    r, body = request_with_retries(url, None)
    if not r.ok:
        raise RuntimeError(json.dumps(body))
    acc = body["adaccounts"][0]["adaccount"]
    return acc.get("name"), acc.get("currency", "USD"), acc.get("timezone", "UTC")


def snap_fetch_entities(account_id, kind, item_key):
    url = f"{API_BASE}/adaccounts/{account_id}/{kind}"
    params = {"limit": 1000}
    items = []
    while True:
        r, body = request_with_retries(url, params)
        if not r.ok:
            raise RuntimeError(json.dumps(body))
        for wrapper in body.get(kind, []):
            items.append(wrapper[item_key])
        next_link = (body.get("paging") or {}).get("next_link")
        if not next_link:
            return items
        url = next_link
        params = None
        time.sleep(REQUEST_DELAY)


def fmt_time(d, tz):
    return dt.datetime(d.year, d.month, d.day, tzinfo=tz).isoformat(timespec="milliseconds")


def fetch_stats_chunk(account_id, chunk_start, chunk_end, tz):
    url = f"{API_BASE}/adaccounts/{account_id}/stats"
    params = {
        "granularity": "DAY",
        "breakdown": "ad",
        "fields": FIELDS_PARAM,
        "start_time": fmt_time(chunk_start, tz),
        "end_time": fmt_time(chunk_end + dt.timedelta(days=1), tz),
        "omit_empty": "true",
        "swipe_up_attribution_window": SWIPE_UP_ATTRIBUTION_WINDOW,
        "view_attribution_window": VIEW_ATTRIBUTION_WINDOW,
    }
    r, body = request_with_retries(url, params)
    if not r.ok:
        raise RuntimeError(json.dumps(body))

    points = []
    for ts in body.get("timeseries_stats", []):
        stat = ts.get("timeseries_stat", {})
        entities = (stat.get("breakdown_stats") or {}).get("ad", [])
        for entity in entities:
            ad_id = entity.get("id")
            for point in entity.get("timeseries", []):
                points.append((ad_id, point.get("start_time"), point.get("stats") or {}))
    return points


def snap_fetch_stats(account_id, since, until, tz):
    all_points = []
    chunk_start = since
    while chunk_start <= until:
        chunk_end = min(chunk_start + dt.timedelta(days=CHUNK_DAYS - 1), until)
        points = fetch_stats_chunk(account_id, chunk_start, chunk_end, tz)
        all_points.extend(points)
        print(f"{account_id} {chunk_start.isoformat()}..{chunk_end.isoformat()}: {len(points)} points", flush=True)
        chunk_start = chunk_end + dt.timedelta(days=1)
        time.sleep(REQUEST_DELAY)
    return all_points


def build_rows(points, account_id, ad_map, adsquad_map, campaign_map, currency, extracted_at):
    ad_rows = []
    conversion_rows = []
    for ad_id, start_time, stats in points:
        if not start_time:
            continue
        date = start_time[:10]
        ad = ad_map.get(ad_id, {})
        adsquad_id = ad.get("ad_squad_id")
        squad = adsquad_map.get(adsquad_id, {})
        campaign_id = squad.get("campaign_id")

        base = {
            "date": date,
            "account_id": account_id,
            "campaign_id": safe_str(campaign_id),
            "campaign_name": campaign_map.get(campaign_id),
            "adsquad_id": safe_str(adsquad_id),
            "adsquad_name": squad.get("name"),
            "ad_id": safe_str(ad_id),
            "ad_name": ad.get("name"),
            "currency": currency,
            "source": SOURCE,
        }

        ad_rows.append({
            **base,
            "spend": micros_to_units(stats.get("spend")),
            "impressions": safe_int(stats.get("impressions")),
            "swipes": safe_int(stats.get("swipes")),
            "extracted_at": extracted_at,
        })

        for field, action_type in CONVERSION_FIELDS:
            count = safe_int(stats.get(field))
            if not count:
                continue
            conversion_rows.append({
                **base,
                "action_type": action_type,
                "value": count,
                "action_value": micros_to_units(stats.get(f"{field}_value")),
                "extracted_at": extracted_at,
            })
    return ad_rows, conversion_rows


def base_schema():
    return [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("account_id", "STRING"),
        bigquery.SchemaField("campaign_id", "STRING"),
        bigquery.SchemaField("campaign_name", "STRING"),
        bigquery.SchemaField("adsquad_id", "STRING"),
        bigquery.SchemaField("adsquad_name", "STRING"),
        bigquery.SchemaField("ad_id", "STRING"),
        bigquery.SchemaField("ad_name", "STRING"),
        bigquery.SchemaField("currency", "STRING"),
        bigquery.SchemaField("source", "STRING"),
    ]


def ad_schema():
    return base_schema() + [
        bigquery.SchemaField("spend", "FLOAT"),
        bigquery.SchemaField("impressions", "INTEGER"),
        bigquery.SchemaField("swipes", "INTEGER"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]


def conversions_schema():
    return base_schema() + [
        bigquery.SchemaField("action_type", "STRING"),
        bigquery.SchemaField("value", "INTEGER"),
        bigquery.SchemaField("action_value", "FLOAT"),
        bigquery.SchemaField("extracted_at", "TIMESTAMP"),
    ]


def ensure_table(client, table_id, schema):
    table = bigquery.Table(table_id, schema=schema)
    table.time_partitioning = bigquery.TimePartitioning(field="date")
    client.create_table(table, exists_ok=True)


def delete_window(client, table_id, account_ids, since, until):
    query = f"""
    DELETE FROM `{table_id}`
    WHERE source = @source
      AND account_id IN UNNEST(@accounts)
      AND `date` BETWEEN @since AND @until
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("source", "STRING", SOURCE),
            bigquery.ArrayQueryParameter("accounts", "STRING", account_ids),
            bigquery.ScalarQueryParameter("since", "DATE", since),
            bigquery.ScalarQueryParameter("until", "DATE", until),
        ]
    )
    client.query(query, job_config=job_config).result()


def insert_rows(client, table_id, rows, schema):
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=schema,
    )
    client.load_table_from_json(rows, table_id, job_config=job_config).result()


def write_window(client, table_id, schema, rows, account_ids, since, until):
    ensure_table(client, table_id, schema)
    delete_window(client, table_id, account_ids, since, until)
    if rows:
        insert_rows(client, table_id, rows, schema)


def main():
    account_ids = parse_account_ids(SNAP_AD_ACCOUNT_IDS)
    if not account_ids:
        return

    since, until = get_date_range()
    extracted_at = dt.datetime.now(dt.timezone.utc).isoformat()
    print(f"window {since.isoformat()}..{until.isoformat()} accounts={account_ids}", flush=True)

    ad_rows = []
    conversion_rows = []
    pulled_accounts = []
    errors = {}

    for account_id in account_ids:
        try:
            account_name, currency, timezone = snap_fetch_account(account_id)
            tz = ZoneInfo(timezone)
            print(f"{account_id} name={account_name} currency={currency} tz={timezone}", flush=True)

            campaigns = snap_fetch_entities(account_id, "campaigns", "campaign")
            campaign_map = {c["id"]: c.get("name") for c in campaigns}

            adsquads = snap_fetch_entities(account_id, "adsquads", "adsquad")
            adsquad_map = {s["id"]: {"name": s.get("name"), "campaign_id": s.get("campaign_id")} for s in adsquads}

            ads = snap_fetch_entities(account_id, "ads", "ad")
            ad_map = {a["id"]: {"name": a.get("name"), "ad_squad_id": a.get("ad_squad_id")} for a in ads}

            points = snap_fetch_stats(account_id, since, until, tz)
            account_ad_rows, account_conversion_rows = build_rows(points, account_id, ad_map, adsquad_map, campaign_map, currency, extracted_at)
        except Exception as e:
            errors[account_id] = str(e)
            print(f"{account_id} FAILED: {e}", flush=True)
            continue

        ad_rows.extend(account_ad_rows)
        conversion_rows.extend(account_conversion_rows)
        pulled_accounts.append(account_id)
        print(f"{account_id} done", flush=True)

    if not pulled_accounts:
        raise RuntimeError(f"all accounts failed: {json.dumps(errors)}")

    client = bigquery.Client(project=BQ_PROJECT)
    base = f"{BQ_PROJECT}.{BQ_DATASET}"

    write_window(client, f"{base}.{AD_TABLE}", ad_schema(), ad_rows, pulled_accounts, since, until)
    write_window(client, f"{base}.{CONVERSIONS_TABLE}", conversions_schema(), conversion_rows, pulled_accounts, since, until)

    print(f"finished accounts={len(pulled_accounts)} failed={len(errors)} errors={json.dumps(errors) if errors else 'none'}", flush=True)


if __name__ == "__main__":
    main()
