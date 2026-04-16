-- sql/03_advanced_analytics.sql
-- Purpose : 5 advanced analytical queries — RFM, cohorts, rolling revenue,
--           anomaly detection and navigation funnel.
-- Author  : ProjetCloud Team
-- Date    : 2024-06-01

-- ============================================================
-- 1. RFM Segmentation
-- ESTIMATED BYTES SCANNED: ~8 MB (partition filter on last 24 months)
-- ============================================================
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
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    GROUP BY client_id
  ),
  rfm_scored AS (
    SELECT
      *,
      NTILE(4) OVER (ORDER BY recency_days    ASC)  AS r_score,  -- lower = better
      NTILE(4) OVER (ORDER BY frequency       DESC) AS f_score,
      NTILE(4) OVER (ORDER BY monetary        DESC) AS m_score
    FROM rfm_base
  )
SELECT
  client_id,
  recency_days,
  frequency,
  ROUND(monetary, 2)    AS monetary,
  r_score,
  f_score,
  m_score,
  r_score + f_score + m_score AS rfm_total,
  CASE
    WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'Champions'
    WHEN r_score >= 3 AND f_score >= 2                  THEN 'Loyal'
    WHEN r_score <= 2 AND f_score >= 2                  THEN 'At Risk'
    ELSE                                                     'Lost'
  END AS rfm_segment
FROM rfm_scored
ORDER BY rfm_total DESC
LIMIT 1000;

-- ============================================================
-- 2. Monthly cohort analysis (12-month window)
-- ESTIMATED BYTES SCANNED: ~10 MB (partition filters on registration + order dates)
-- ============================================================
WITH
  client_cohort AS (
    SELECT
      client_id,
      DATE_TRUNC(DATE(registration_date), MONTH) AS cohort_month
    FROM `ecommerce_analytics.clients`
    WHERE DATE(registration_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
  ),
  order_with_cohort AS (
    SELECT
      o.client_id,
      c.cohort_month,
      DATE_TRUNC(DATE(o.order_date), MONTH)                     AS order_month,
      DATE_DIFF(
        DATE_TRUNC(DATE(o.order_date), MONTH),
        c.cohort_month,
        MONTH
      )                                                          AS month_index,
      o.total_amount
    FROM `ecommerce_analytics.orders` o
    JOIN client_cohort c USING (client_id)
    WHERE
      o.status != 'Cancelled'
      AND DATE(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
      AND DATE_DIFF(DATE_TRUNC(DATE(o.order_date), MONTH), c.cohort_month, MONTH) BETWEEN 0 AND 11
  )
SELECT
  cohort_month,
  month_index,
  COUNT(DISTINCT client_id) AS active_clients,
  ROUND(SUM(total_amount), 2) AS cohort_revenue
FROM order_with_cohort
GROUP BY cohort_month, month_index
ORDER BY cohort_month, month_index
LIMIT 1000;

-- ============================================================
-- 3. 4-week rolling revenue trend
-- ESTIMATED BYTES SCANNED: ~5 MB (partition filter last 6 months)
-- ============================================================
WITH
  weekly AS (
    SELECT
      DATE_TRUNC(DATE(order_date), WEEK(MONDAY)) AS week,
      SUM(total_amount)                           AS weekly_revenue
    FROM `ecommerce_analytics.orders`
    WHERE
      status != 'Cancelled'
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY week
  )
SELECT
  week,
  ROUND(weekly_revenue, 2) AS weekly_revenue,
  ROUND(
    AVG(weekly_revenue) OVER (
      ORDER BY week
      ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
    ),
    2
  ) AS rolling_4w_avg_revenue
FROM weekly
ORDER BY week
LIMIT 1000;

-- ============================================================
-- 4. Regional anomaly detection (orders > mean + 2 * stddev)
-- ESTIMATED BYTES SCANNED: ~8 MB (partition filter last 12 months)
-- ============================================================
WITH
  stats AS (
    SELECT
      region,
      AVG(total_amount)         AS mean_amount,
      STDDEV_POP(total_amount)  AS stddev_amount
    FROM `ecommerce_analytics.orders`
    WHERE
      status != 'Cancelled'
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY region
  )
SELECT
  o.order_id,
  o.client_id,
  o.region,
  ROUND(o.total_amount, 2)              AS total_amount,
  ROUND(s.mean_amount, 2)               AS region_mean,
  ROUND(s.stddev_amount, 2)             AS region_stddev,
  ROUND(o.total_amount - s.mean_amount, 2) AS deviation,
  ROUND((o.total_amount - s.mean_amount) / NULLIF(s.stddev_amount, 0), 2) AS z_score
FROM `ecommerce_analytics.orders` o
JOIN stats s USING (region)
WHERE
  o.status != 'Cancelled'
  AND DATE(o.order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
  AND o.total_amount > s.mean_amount + 2 * s.stddev_amount
ORDER BY deviation DESC
LIMIT 1000;

-- ============================================================
-- 5. Navigation conversion funnel: /products → /cart → /checkout
-- ESTIMATED BYTES SCANNED: ~12 MB (partition filter last 12 months)
-- ============================================================
WITH
  page_agg AS (
    SELECT
      DATE(event_datetime)          AS event_date,
      page,
      COUNT(DISTINCT session_id)    AS session_count,
      COUNT(DISTINCT client_id)     AS user_count
    FROM `ecommerce_analytics.page_views`
    WHERE DATE(event_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
    GROUP BY event_date, page
  ),
  funnel AS (
    SELECT
      event_date,
      COALESCE(SUM(CASE WHEN page = '/products'  THEN session_count END), 0) AS products_sessions,
      COALESCE(SUM(CASE WHEN page = '/cart'      THEN session_count END), 0) AS cart_sessions,
      COALESCE(SUM(CASE WHEN page = '/checkout'  THEN session_count END), 0) AS checkout_sessions
    FROM page_agg
    GROUP BY event_date
  )
SELECT
  event_date,
  products_sessions,
  cart_sessions,
  checkout_sessions,
  SAFE_DIVIDE(cart_sessions,     NULLIF(products_sessions, 0)) * 100 AS pct_products_to_cart,
  SAFE_DIVIDE(checkout_sessions, NULLIF(cart_sessions, 0))     * 100 AS pct_cart_to_checkout,
  SAFE_DIVIDE(checkout_sessions, NULLIF(products_sessions, 0)) * 100 AS overall_conversion_pct
FROM funnel
ORDER BY event_date
LIMIT 1000;
