"""TPC-H Snowpark analytics for a Snowflake Python Worksheet.

In Snowsight, create a Python Worksheet, select TPCH_ADMIN / TPCH_WH /
TPCH_ANALYTICS_DB / ANALYTICS, paste this file, set handler to ``main`` and run.
No local credentials are required because Snowflake injects the Session.
"""

from snowflake.snowpark import Session, Window
from snowflake.snowpark import functions as F


DATABASE = "TPCH_ANALYTICS_DB"


def build_customer_rfm(session: Session):
    """Calculate RFM metrics, quintile scores and readable segments."""
    customers = session.table(f"{DATABASE}.SILVER.CUSTOMER_SILVER").select(
        "C_CUSTKEY", "C_NAME"
    )
    orders = session.table(f"{DATABASE}.SILVER.ORDERS_SILVER").select(
        "O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_TOTALPRICE"
    )

    metrics = (
        customers.join(
            orders,
            customers["C_CUSTKEY"] == orders["O_CUSTKEY"],
            "left",
        )
        .group_by(customers["C_CUSTKEY"], customers["C_NAME"])
        .agg(
            F.max("O_ORDERDATE").alias("LAST_ORDER_DATE"),
            F.count("O_ORDERKEY").alias("FREQUENCY"),
            F.coalesce(F.sum("O_TOTALPRICE"), F.lit(0)).alias("MONETARY"),
        )
        .with_column(
            "RECENCY_DAYS",
            F.when(
                F.col("LAST_ORDER_DATE").is_null(), F.lit(999999)
            ).otherwise(
                F.datediff("day", F.col("LAST_ORDER_DATE"), F.current_date())
            ),
        )
    )

    recency_window = Window.order_by(F.col("RECENCY_DAYS").asc())
    frequency_window = Window.order_by(F.col("FREQUENCY").asc())
    monetary_window = Window.order_by(F.col("MONETARY").asc())

    scored = (
        metrics.with_column("R_SCORE", 6 - F.ntile(5).over(recency_window))
        .with_column("F_SCORE", F.ntile(5).over(frequency_window))
        .with_column("M_SCORE", F.ntile(5).over(monetary_window))
        .with_column(
            "RFM_SCORE",
            F.col("R_SCORE") * 100 + F.col("F_SCORE") * 10 + F.col("M_SCORE"),
        )
        .with_column(
            "RFM_SEGMENT",
            F.when(
                (F.col("R_SCORE") >= 4)
                & (F.col("F_SCORE") >= 4)
                & (F.col("M_SCORE") >= 4),
                F.lit("CHAMPIONS"),
            )
            .when(
                (F.col("R_SCORE") >= 3) & (F.col("F_SCORE") >= 3),
                F.lit("LOYAL"),
            )
            .when(
                (F.col("R_SCORE") >= 4) & (F.col("F_SCORE") <= 2),
                F.lit("NEW_OR_PROMISING"),
            )
            .when(F.col("R_SCORE") <= 2, F.lit("AT_RISK"))
            .otherwise(F.lit("NEEDS_ATTENTION")),
        )
    )

    scored.write.mode("overwrite").save_as_table(
        f"{DATABASE}.ANALYTICS.CUSTOMER_RFM_SCORES"
    )
    return scored


def analyze_sales_trend(session: Session):
    """Aggregate monthly sales and calculate month-over-month growth."""
    orders = session.table(f"{DATABASE}.SILVER.ORDERS_SILVER")
    monthly = (
        orders.with_column(
            "ORDER_MONTH", F.date_trunc("month", F.col("O_ORDERDATE"))
        )
        .group_by("ORDER_MONTH")
        .agg(
            F.count("O_ORDERKEY").alias("ORDER_COUNT"),
            F.sum("O_TOTALPRICE").alias("TOTAL_REVENUE"),
            F.avg("O_TOTALPRICE").alias("AVG_ORDER_VALUE"),
            F.count_distinct("O_CUSTKEY").alias("ACTIVE_CUSTOMERS"),
        )
    )

    trend_window = Window.order_by("ORDER_MONTH")
    trend = (
        monthly.with_column(
            "PREVIOUS_MONTH_REVENUE",
            F.lag("TOTAL_REVENUE", 1).over(trend_window),
        )
        .with_column(
            "MOM_GROWTH_PCT",
            F.when(
                F.col("PREVIOUS_MONTH_REVENUE").is_null()
                | (F.col("PREVIOUS_MONTH_REVENUE") == 0),
                F.lit(None),
            ).otherwise(
                F.round(
                    (
                        F.col("TOTAL_REVENUE")
                        / F.col("PREVIOUS_MONTH_REVENUE")
                        - 1
                    )
                    * 100,
                    2,
                )
            ),
        )
        .sort("ORDER_MONTH")
    )

    trend.write.mode("overwrite").save_as_table(
        f"{DATABASE}.ANALYTICS.SALES_TREND_MONTHLY"
    )
    return trend


def main(session: Session):
    """Python Worksheet handler returning a visible one-row DataFrame."""
    rfm = build_customer_rfm(session)
    rfm_count = rfm.count()

    trend = analyze_sales_trend(session)
    trend_count = trend.count()

    return session.create_dataframe(
        [["SUCCESS", rfm_count, trend_count]],
        schema=["STATUS", "RFM_ROWS", "TREND_ROWS"],
    )
