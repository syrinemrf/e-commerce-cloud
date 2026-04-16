-- sql/02_create_views.sql
-- Purpose : 7 analytical views covering revenue, retention, products,
--           incidents, navigation, weekly KPIs and client 360.
-- Author  : ProjetCloud Team
-- Date    : 2024-06-01
--
-- COST NOTE: Views do not store data. Query costs depend on underlying tables.
-- All views filter on partition columns to minimize bytes scanned.
-- Never run views without partition filters in production.

-- ============================================================
-- 1. v_revenue_by_region
-- Total revenue, order count, average basket, % of global revenue, MoM growth
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_revenue_by_region` AS
WITH
  base AS (
    SELECT
      region,
      DATE_TRUNC(DATE(order_date), MONTH) AS order_month,
      SUM(total_amount)                   AS monthly_revenue,
      COUNT(*)                            AS order_count
    FROM `ecommerce_analytics.orders`
    WHERE
      status != 'Cancelled'
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 13 MONTH)
    GROUP BY region, order_month
  ),
  global_rev AS (
    SELECT SUM(monthly_revenue) AS total_global FROM base
  ),
  with_growth AS (
    SELECT
      b.*,
      g.total_global,
      SAFE_DIVIDE(b.monthly_revenue, NULLIF(g.total_global, 0)) * 100 AS pct_global_revenue,
      SAFE_DIVIDE(
        b.monthly_revenue - LAG(b.monthly_revenue) OVER (PARTITION BY b.region ORDER BY b.order_month),
        NULLIF(LAG(b.monthly_revenue) OVER (PARTITION BY b.region ORDER BY b.order_month), 0)
      ) * 100 AS mom_growth_pct
    FROM base b, global_rev g
  )
SELECT
  region,
  order_month,
  monthly_revenue,
  order_count,
  SAFE_DIVIDE(monthly_revenue, NULLIF(order_count, 0)) AS avg_basket,
  pct_global_revenue,
  mom_growth_pct
FROM with_growth
ORDER BY region, order_month;

-- ============================================================
-- 2. v_inactive_clients
-- Clients with no order in last 60 days
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_inactive_clients` AS
WITH
  last_order AS (
    SELECT
      client_id,
      MAX(DATE(order_date)) AS last_purchase_date,
      SUM(total_amount)     AS historical_revenue,
      COUNT(*)              AS order_count
    FROM `ecommerce_analytics.orders`
    WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    GROUP BY client_id
  ),
  incident_counts AS (
    SELECT
      client_id,
      COUNT(*) AS incident_count
    FROM `ecommerce_analytics.incidents`
    WHERE DATE(report_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    GROUP BY client_id
  )
SELECT
  c.client_id,
  c.email,
  c.country,
  c.segment,
  lo.last_purchase_date,
  lo.historical_revenue,
  lo.order_count,
  COALESCE(ic.incident_count, 0) AS incident_count
FROM `ecommerce_analytics.clients` c
JOIN last_order lo USING (client_id)
LEFT JOIN incident_counts ic USING (client_id)
WHERE lo.last_purchase_date < DATE_SUB(CURRENT_DATE(), INTERVAL 60 DAY)
ORDER BY lo.historical_revenue DESC;

-- ============================================================
-- 3. v_top_products
-- Revenue, quantity sold, distinct order count, cancellation rate
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_top_products` AS
WITH
  product_stats AS (
    SELECT
      oi.product_id,
      p.product_name,
      p.category,
      SUM(oi.quantity * oi.unit_price)                     AS revenue,
      SUM(oi.quantity)                                     AS total_quantity,
      COUNT(DISTINCT oi.order_id)                          AS distinct_order_count,
      COUNTIF(o.status = 'Cancelled')                      AS cancelled_count,
      COUNT(*)                                             AS total_items
    FROM `ecommerce_analytics.order_items` oi
    JOIN `ecommerce_analytics.orders`  o USING (order_id)
    JOIN `ecommerce_analytics.products` p USING (product_id)
    WHERE oi.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
    GROUP BY oi.product_id, p.product_name, p.category
  )
SELECT
  product_id,
  product_name,
  category,
  revenue,
  total_quantity,
  distinct_order_count,
  SAFE_DIVIDE(cancelled_count, NULLIF(total_items, 0)) * 100 AS cancellation_rate_pct,
  RANK() OVER (PARTITION BY category ORDER BY revenue DESC)  AS category_rank
FROM product_stats
ORDER BY revenue DESC;

-- ============================================================
-- 4. v_recurring_incidents
-- Per category: count, avg resolution, % escalated, % critical, % linked to order
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_recurring_incidents` AS
SELECT
  category,
  COUNT(*)                                                         AS total_incidents,
  ROUND(AVG(resolution_time_h), 2)                                AS avg_resolution_time_h,
  SAFE_DIVIDE(COUNTIF(status = 'Escalated'), COUNT(*)) * 100      AS pct_escalated,
  SAFE_DIVIDE(COUNTIF(priority = 'Critical'), COUNT(*)) * 100     AS pct_critical,
  SAFE_DIVIDE(COUNTIF(order_id IS NOT NULL), COUNT(*)) * 100      AS pct_linked_to_order
FROM `ecommerce_analytics.incidents`
WHERE DATE(report_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 24 MONTH)
GROUP BY category
ORDER BY total_incidents DESC;

-- ============================================================
-- 5. v_navigation_funnel
-- Per page: session count, avg duration, unique users, device split, engagement score
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_navigation_funnel` AS
SELECT
  page,
  COUNT(*)                                                           AS session_count,
  ROUND(AVG(duration_seconds), 2)                                   AS avg_duration_seconds,
  COUNT(DISTINCT client_id)                                          AS unique_users,
  SAFE_DIVIDE(COUNTIF(device = 'Mobile'), COUNT(*)) * 100           AS pct_mobile,
  SAFE_DIVIDE(COUNTIF(device = 'Desktop'), COUNT(*)) * 100          AS pct_desktop,
  ROUND(AVG(duration_seconds) * COUNT(*) / 1000.0, 4)               AS engagement_score
FROM `ecommerce_analytics.page_views`
WHERE DATE(event_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH)
GROUP BY page
ORDER BY session_count DESC;

-- ============================================================
-- 6. v_weekly_kpis
-- Per ISO week: revenue, order count, new clients, incident count, WoW delta
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_weekly_kpis` AS
WITH
  orders_weekly AS (
    SELECT
      DATE_TRUNC(DATE(order_date), WEEK(MONDAY)) AS week,
      SUM(total_amount)                           AS revenue,
      COUNT(*)                                    AS order_count
    FROM `ecommerce_analytics.orders`
    WHERE
      status != 'Cancelled'
      AND DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY week
  ),
  clients_weekly AS (
    SELECT
      DATE_TRUNC(DATE(registration_date), WEEK(MONDAY)) AS week,
      COUNT(*)                                           AS new_clients
    FROM `ecommerce_analytics.clients`
    WHERE DATE(registration_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY week
  ),
  incidents_weekly AS (
    SELECT
      DATE_TRUNC(DATE(report_date), WEEK(MONDAY)) AS week,
      COUNT(*)                                     AS incident_count
    FROM `ecommerce_analytics.incidents`
    WHERE DATE(report_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 6 MONTH)
    GROUP BY week
  )
SELECT
  ow.week,
  ow.revenue,
  ow.order_count,
  COALESCE(cw.new_clients, 0)    AS new_clients,
  COALESCE(iw.incident_count, 0) AS incident_count,
  ow.revenue - LAG(ow.revenue) OVER (ORDER BY ow.week) AS wow_revenue_delta
FROM orders_weekly ow
LEFT JOIN clients_weekly  cw USING (week)
LEFT JOIN incidents_weekly iw USING (week)
ORDER BY ow.week;

-- ============================================================
-- 7. v_client_360
-- One row per client with aggregated KPIs and computed value segment
-- ============================================================
CREATE OR REPLACE VIEW `ecommerce_analytics.v_client_360` AS
WITH
  order_stats AS (
    SELECT
      client_id,
      SUM(total_amount)                AS total_revenue,
      COUNT(*)                         AS order_count,
      SAFE_DIVIDE(SUM(total_amount), COUNT(*)) AS avg_basket
    FROM `ecommerce_analytics.orders`
    WHERE DATE(order_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY client_id
  ),
  incident_stats AS (
    SELECT
      client_id,
      COUNT(*)                         AS incident_count,
      APPROX_TOP_COUNT(category, 1)[OFFSET(0)].value AS top_incident_category
    FROM `ecommerce_analytics.incidents`
    WHERE DATE(report_date) >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY client_id
  ),
  nav_stats AS (
    SELECT
      client_id,
      COUNT(*)                                               AS pages_visited,
      APPROX_TOP_COUNT(page, 1)[OFFSET(0)].value             AS favorite_page,
      APPROX_TOP_COUNT(device, 1)[OFFSET(0)].value           AS primary_device
    FROM `ecommerce_analytics.page_views`
    WHERE
      client_id IS NOT NULL
      AND DATE(event_datetime) >= DATE_SUB(CURRENT_DATE(), INTERVAL 36 MONTH)
    GROUP BY client_id
  ),
  scored AS (
    SELECT
      c.client_id,
      c.email,
      c.country,
      c.segment,
      COALESCE(os.total_revenue, 0)          AS revenue,
      COALESCE(os.order_count, 0)            AS order_count,
      COALESCE(os.avg_basket, 0)             AS avg_basket,
      COALESCE(is2.incident_count, 0)        AS incident_count,
      is2.top_incident_category,
      COALESCE(ns.pages_visited, 0)          AS pages_visited,
      ns.favorite_page,
      ns.primary_device,
      ROUND(
        COALESCE(os.total_revenue, 0) * 0.5
        + SAFE_DIVIDE(1.0, NULLIF(COALESCE(is2.incident_count, 0), 0)) * 20
        + COALESCE(os.order_count, 0) * 2,
        2
      ) AS value_score
    FROM `ecommerce_analytics.clients` c
    LEFT JOIN order_stats    os   USING (client_id)
    LEFT JOIN incident_stats is2  USING (client_id)
    LEFT JOIN nav_stats       ns  USING (client_id)
  )
SELECT
  *,
  CASE
    WHEN value_score > 200  THEN 'VIP'
    WHEN value_score >= 50  THEN 'Regular'
    ELSE                         'At risk'
  END AS value_segment
FROM scored
ORDER BY value_score DESC;
