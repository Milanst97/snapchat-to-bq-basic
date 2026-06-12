# Snapchat Ads to BigQuery (basic)

Generic Cloud Run Job that pulls daily ad-level performance and conversions from the Snapchat Marketing API into BigQuery. Stripped version of the Sandbox VR pipeline: no location mapping, no store_type, no currency conversion, no DMA. Use as the starting template for new clients.

## What it does

1. Refreshes a Snapchat access token via the OAuth refresh token flow (auto refresh, retry on 401).
2. Per ad account: reads name, currency and timezone, fetches campaigns, ad squads and ads lists to map IDs to names.
3. Pulls daily stats from `/v1/adaccounts/{id}/stats` with `breakdown=ad`.
4. Converts micro-currency to units. Amounts stay in the account's local currency, the `currency` column says which.
5. Deletes the window for the pulled accounts and reinserts.

## Tables

All in `BQ_PROJECT.BQ_DATASET`, partitioned on `date`.

- `snapchat_ads`: ad level. spend, impressions, swipes.
- `snapchat_ads_conversions`: tall format, one row per ad/date/action_type. value (count), action_value. Events: view_content, add_cart, start_checkout, purchase, subscribe, reserve, custom_event_1. Extend via `CONVERSION_FIELDS` in code.

## Environment variables

Required: `SNAP_CLIENT_ID`, `SNAP_CLIENT_SECRET`, `SNAP_REFRESH_TOKEN`, `SNAP_AD_ACCOUNT_IDS` (comma separated UUIDs), `BQ_PROJECT`, `BQ_DATASET`.

Optional: `LOOKBACK_DAYS` (default 7), `START_DATE`/`END_DATE` (ISO dates, for backfills), `CHUNK_DAYS` (default 30), `REQUEST_DELAY` (default 1s), `SWIPE_UP_ATTRIBUTION_WINDOW` (default 28_DAY), `VIEW_ATTRIBUTION_WINDOW` (default 1_DAY), `REPORTING_TZ` (default Etc/GMT-1), `BQ_AD_TABLE`, `BQ_CONVERSIONS_TABLE`.

## Deploy

From the project folder in Cloud Shell:

```
gcloud run jobs deploy snapchat-to-bq-<client> --source . --region <region> --project growmojo-database
```

Backfill: run with `START_DATE` and `END_DATE` overrides, then remove them.

## Notes

- Snapchat returns spend and `*_value` fields in micro-currency (divide by 1,000,000), already handled.
- `end_time` on the stats endpoint is exclusive, the code sends `until + 1 day`.
- DAY granularity requests are aligned to the ad account timezone automatically.
- Clicks are called swipes on Snapchat, the column is named `swipes`.
- For multi-currency clients, add the Frankfurter conversion from the Sandbox VR version.
