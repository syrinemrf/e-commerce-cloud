-- sql/04_scheduled_queries.sql
-- Purpose : 2 lightweight scheduled queries for BigQuery Scheduled Queries.
--           Both queries use partition filters to keep cost at $0.
-- Author  : ProjetCloud Team
-- Date    : 2024-06-01
--
-- COST NOTE: BigQuery Scheduled Queries are free to schedule (up to 10/project).
-- Query cost depends on bytes scanned — partition filters keep this near $0.
-- Schedule these via BigQuery Console → Scheduled Queries → Create.

-- ============================================================
-- 1. Daily KPI refresh — filters on last 2 days only
--    Schedule: every day at 06:00 UTC
--    Destination table: ecommerce_analytics.kpis_daily
--    Write disposition: WRITE_TRUNCATE
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.kpis_daily`
PARTITION BY kpi_date
OPTIONS(description="Daily aggregated KPIs — refreshed every morning at 06:00 UTC")
AS
SELECT
  DATE(order_date)             AS kpi_date,
  COUNT(*)                     AS total_orders,
  ROUND(SUM(total_amount), 2)  AS total_revenue,
  ROUND(AVG(total_amount), 2)  AS avg_basket,
  COUNTIF(status = 'Delivered') AS delivered_orders,
  COUNTIF(status = 'Cancelled') AS cancelled_orders,
  COUNTIF(status = 'Refunded')  AS refunded_orders
FROM `ecommerce_analytics.orders`
WHERE
  -- Partition filter: only scan last 2 days to avoid full table scan
  DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 DAY)
GROUP BY kpi_date;

-- ============================================================
-- 2. Weekly RFM segmentation update
--    Schedule: every Monday at 05:00 UTC
--    Destination table: ecommerce_analytics.rfm_weekly
--    Write disposition: WRITE_TRUNCATE
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.rfm_weekly`
OPTIONS(description="Weekly RFM segmentation — refreshed every Monday at 05:00 UTC")
AS
WITH
  rfm_base AS (
    SELECT
      client_id,
      DATE_DIFF(CURRENT_DATE(), MAX(DATE(order_date)), DAY) AS recency_days,
      COUNT(*)                                               AS frequency,
      SUM(total_amount)                                      AS monetary
    FROM `ecommerce_analytics.orders`
    WHERE
      status != 'Cancelled'
      -- Partition filter: last 90 days only
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
    GROUP BY client_id
  ),
  rfm_scored AS (
    SELECT
      *,
      NTILE(4) OVER (ORDER BY recency_days ASC)  AS r_score,
      NTILE(4) OVER (ORDER BY frequency    DESC) AS f_score,
      NTILE(4) OVER (ORDER BY monetary     DESC) AS m_score
    FROM rfm_base
  )
SELECT
  client_id,
  recency_days,
  frequency,
  ROUND(monetary, 2) AS monetary,
  r_score,
  f_score,
  m_score,
  r_score + f_score + m_score AS rfm_total,
  CASE
    WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Champions'
    WHEN r_score >= 3 AND f_score >= 2                  THEN 'Loyal'
    WHEN r_score <= 2 AND f_score >= 2                  THEN 'At Risk'
    ELSE                                                     'Lost'
  END AS rfm_segment,
  CURRENT_DATETIME() AS computed_at
FROM rfm_scored;
