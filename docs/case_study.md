# Case Study: Snowflake TPC-H Analytics Platform

## Problem

Build a reproducible analytics platform that ingests TPC-H data, separates raw
and curated processing, responds to incremental changes, enforces access
control, validates data quality, and exposes business-ready outputs to Python
and BI consumers.

## Solution

The implementation uses a Medallion-style architecture:

- `STAGING` stores eight source-aligned Bronze tables, internal stage objects,
  file formats, pipes, and source streams.
- `SILVER` contains standardized customer, order, and line-item entities.
- `GOLD` contains customer lifetime value and daily sales aggregates.
- `ANALYTICS` contains Snowpark-generated RFM scores and monthly trends.
- `REPORTS` provides controlled, read-only consumption views.

Five dependent Snowflake Tasks execute stored procedures in a deterministic
order. Each procedure performs an idempotent `MERGE`, while the root task checks
`SYSTEM$STREAM_HAS_DATA` to avoid unnecessary compute.

## Incremental verification

A new one-row order file was uploaded to the internal stage. The record with
key `900000001` was loaded into Bronze, consumed by the orders stream, merged
into `SILVER.ORDERS_SILVER`, and reflected in customer-LTV and daily-sales Gold
outputs. Task history showed all five tasks succeeded, and stream checks were
`FALSE` afterward, confirming that the changes had been consumed.

## Quality controls

The project tests:

- required-column NULLs;
- duplicate business keys;
- orphaned foreign keys;
- invalid status and numeric domains;
- delivery and date relationships;
- reconciliation between Bronze, Silver, and Gold.

The baseline run recorded zero required violations and zero reconciliation
mismatches.

## Analytics outputs

Snowpark produces 150,000 customer RFM rows and 80 monthly sales-trend rows.
The Power BI report summarizes 226.8B in revenue, 1.50M orders, 150K customers,
regional customer value, customer tiers, and RFM segment distribution.

## Tradeoffs and next steps

- The initial load uses Snowflake sample data for reproducibility; the file
  ingestion route is verified separately with an incremental fixture.
- SF1 is too small to justify a clustering-key claim without a controlled
  experiment, so the project records Query Profile metrics without asserting
  an optimization benefit.
- A production version would add orchestration alerts, CI against a temporary
  Snowflake environment, schema contracts, and infrastructure as code.
