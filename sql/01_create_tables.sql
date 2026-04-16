-- sql/01_create_tables.sql
-- Purpose : Create all 6 BigQuery tables with partitioning, clustering and column descriptions.
-- Author  : ProjetCloud Team
-- Date    : 2024-06-01
--
-- COST NOTE: Tables are partitioned to minimize query bytes scanned.
-- Always use WHERE clauses on partition columns in production queries.
-- Partition pruning can reduce query cost by 90–99% on large tables.

-- ============================================================
-- 1. clients
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.clients`
(
  client_id         STRING  OPTIONS(description="Unique client identifier (C0001–C2000)"),
  last_name         STRING  OPTIONS(description="Client last name"),
  first_name        STRING  OPTIONS(description="Client first name"),
  email             STRING  OPTIONS(description="Client email address"),
  age               INT64   OPTIONS(description="Client age in years"),
  gender            STRING  OPTIONS(description="Gender: M / F / Non-binary"),
  country           STRING  OPTIONS(description="Client country of residence"),
  city              STRING  OPTIONS(description="Client city (consistent with country)"),
  phone             STRING  OPTIONS(description="Client phone number"),
  registration_date DATETIME OPTIONS(description="Date and time of account creation"),
  segment           STRING  OPTIONS(description="Client segment: new or regular")
)
PARTITION BY DATE(registration_date)
CLUSTER BY country, segment
OPTIONS(
  description="Client master table — partitioned by registration date"
);

-- ============================================================
-- 2. products
-- (no partition — small dimension table < 1 MB)
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.products`
(
  product_id   STRING  OPTIONS(description="Unique product identifier (P001–P050)"),
  product_name STRING  OPTIONS(description="Product display name"),
  category     STRING  OPTIONS(description="Product category: Electronics / Audio / Office Furniture / Accessories / Storage"),
  unit_price   FLOAT64 OPTIONS(description="Base unit price in EUR"),
  stock        INT64   OPTIONS(description="Current stock quantity")
)
OPTIONS(
  description="Product catalogue — no partition (< 1 MB dimension table)"
);

-- ============================================================
-- 3. orders
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.orders`
(
  order_id       STRING   OPTIONS(description="Unique order identifier (ORD00001–ORD15000)"),
  client_id      STRING   OPTIONS(description="FK → clients.client_id"),
  order_date     DATETIME OPTIONS(description="Order creation datetime"),
  status         STRING   OPTIONS(description="Order status: Delivered / Pending / Cancelled / Refunded"),
  payment_method STRING   OPTIONS(description="Payment method used"),
  region         STRING   OPTIONS(description="Geographic region derived from client country"),
  total_amount   FLOAT64  OPTIONS(description="Total order amount in EUR (recomputed from items)")
)
PARTITION BY DATE(order_date)
CLUSTER BY status, region
OPTIONS(
  description="Orders fact table — partitioned by order date"
);

-- ============================================================
-- 4. order_items
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.order_items`
(
  item_id    STRING  OPTIONS(description="Unique line-item identifier"),
  order_id   STRING  OPTIONS(description="FK → orders.order_id"),
  order_date DATE    OPTIONS(description="Partition column — must match orders.order_date"),
  product_id STRING  OPTIONS(description="FK → products.product_id"),
  quantity   INT64   OPTIONS(description="Quantity ordered (1–4)"),
  unit_price FLOAT64 OPTIONS(description="Actual unit price after discount (±5%)")
)
PARTITION BY order_date
CLUSTER BY product_id
OPTIONS(
  description="Order line items — partitioned by order date"
);

-- ============================================================
-- 5. incidents
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.incidents`
(
  incident_id       STRING   OPTIONS(description="Unique incident identifier (INC0001–INC3000)"),
  client_id         STRING   OPTIONS(description="FK → clients.client_id"),
  report_date       DATETIME OPTIONS(description="Date and time the incident was reported"),
  category          STRING   OPTIONS(description="Incident category: Payment / Delivery / Defective product / Login / Customer service"),
  order_id          STRING   OPTIONS(description="FK → orders.order_id (nullable — 70% linked)"),
  status            STRING   OPTIONS(description="Incident status: Resolved / In progress / Escalated / Closed"),
  priority          STRING   OPTIONS(description="Incident priority: Low / Medium / High / Critical"),
  resolution_time_h FLOAT64  OPTIONS(description="Hours to resolution (NULL if In progress)")
)
PARTITION BY DATE(report_date)
CLUSTER BY category, priority
OPTIONS(
  description="Customer incidents — partitioned by report date"
);

-- ============================================================
-- 6. page_views
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.page_views`
(
  session_id       STRING   OPTIONS(description="Unique session identifier (S000001–S050000)"),
  client_id        STRING   OPTIONS(description="FK → clients.client_id (NULL for anonymous visitors)"),
  page             STRING   OPTIONS(description="Page URL visited"),
  event_datetime   DATETIME OPTIONS(description="Event timestamp"),
  duration_seconds INT64    OPTIONS(description="Time spent on page in seconds"),
  device           STRING   OPTIONS(description="Device type: Mobile / Desktop / Tablet"),
  browser          STRING   OPTIONS(description="Browser used"),
  traffic_source   STRING   OPTIONS(description="Traffic origin: Direct / Google / Instagram / Email / Referral")
)
PARTITION BY DATE(event_datetime)
CLUSTER BY page, device
OPTIONS(
  description="Web page views — partitioned by event date"
);

-- ============================================================
-- 7. pipeline_errors (Beam error sink)
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.pipeline_errors`
(
  raw_message          STRING   OPTIONS(description="Raw JSON message that failed validation"),
  error_reason         STRING   OPTIONS(description="Validation failure reason"),
  processing_timestamp DATETIME OPTIONS(description="Timestamp when the error was detected")
)
PARTITION BY DATE(processing_timestamp)
OPTIONS(
  description="Beam pipeline validation errors — partitioned by processing timestamp"
);

-- ============================================================
-- 8. orders_stream (Beam streaming sink)
-- ============================================================
CREATE OR REPLACE TABLE `ecommerce_analytics.orders_stream`
(
  order_id             STRING   OPTIONS(description="Order identifier from Pub/Sub message"),
  client_id            STRING   OPTIONS(description="Client identifier"),
  total_amount         FLOAT64  OPTIONS(description="Order total in EUR"),
  status               STRING   OPTIONS(description="Order status"),
  sent_at              DATETIME OPTIONS(description="Original message publish timestamp"),
  processing_timestamp DATETIME OPTIONS(description="Timestamp when Beam processed the message")
)
PARTITION BY DATE(processing_timestamp)
OPTIONS(
  description="Real-time orders received via Pub/Sub → Beam → BigQuery"
);
