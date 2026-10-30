-- Set catalog/schema to the SAME widget values used for the pipeline.
USE CATALOG workspace;
USE SCHEMA payments_demo;

SELECT payment_date,
       SUM(attempted_count) AS attempted_payments,
       SUM(successful_count) AS successful_payments,
       ROUND(100.0 * SUM(successful_count) / NULLIF(SUM(attempted_count),0),2) AS success_rate_pct,
       SUM(successful_value_kobo) / 100.0 AS successful_value_ngn,
       SUM(fee_revenue_kobo) / 100.0 AS synthetic_fee_revenue_ngn
FROM gold_daily_payments
GROUP BY payment_date ORDER BY payment_date;

SELECT channel, SUM(successful_value_kobo)/100.0 AS successful_value_ngn,
       SUM(fee_revenue_kobo)/100.0 AS synthetic_fee_revenue_ngn
FROM gold_daily_payments GROUP BY channel;

SELECT reason, COUNT(*) AS rejected_records FROM quarantine_payments GROUP BY reason;
SELECT * FROM batch_quality;
