-- New events_daily DDL: identical columns, but the ORDER BY drops the
-- high-cardinality `url` from the key (fewer key columns → the aggregate
-- collapses more rows; sum totals are unchanged). The rebuilder renames this to
-- events_daily_v2 during the rebuild.
--
-- For reference, the feeding MV would look like:
--   CREATE MATERIALIZED VIEW events_daily_mv TO events_daily
--   (date Date, country LowCardinality(String), url String,
--    hits UInt64, revenue Decimal(38, 4)) AS
--   SELECT date, country, url, count() AS hits, sum(revenue) AS revenue
--   FROM events GROUP BY date, country, url;
CREATE TABLE IF NOT EXISTS events_daily
(
    date     Date,
    country  LowCardinality(String),
    url      String,
    hits     SimpleAggregateFunction(sum, UInt64),
    revenue  SimpleAggregateFunction(sum, Decimal(38, 4))
)
ENGINE = AggregatingMergeTree
PARTITION BY toYYYYMM(date)
ORDER BY (date, country)
TTL date + toIntervalMonth(6);
