"""TPC-H analytics with Snowpark Python.

Creates two analytical outputs:
  * ANALYTICS.CUSTOMER_RFM_SCORES
  * ANALYTICS.SALES_TREND_MONTHLY

Authentication uses a named connection from ~/.snowflake/connections.toml by
default. Environment variables are supported as a portable fallback.
"""

from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path
import tomllib
from typing import Dict

from snowflake.snowpark import Session, Window
from snowflake.snowpark import functions as F


DATABASE = "TPCH_ANALYTICS_DB"
WAREHOUSE = "TPCH_WH"


def create_session(connection_name: str, password_auth: bool = False) -> Session:
    """Create a Snowpark session from a named connection or environment.

    ``password_auth`` reads the non-secret values from connections.toml and asks
    for the password with hidden terminal input. The password is never written
    to disk or included in the command line.
    """
    if connection_name:
        if password_auth:
            config_path = Path.home() / ".snowflake" / "connections.toml"
            if not config_path.exists():
                raise FileNotFoundError(f"Connection file not found: {config_path}")

            with config_path.open("rb") as config_file:
                profiles = tomllib.load(config_file)
            if connection_name not in profiles:
                raise KeyError(
                    f"Connection profile [{connection_name}] not found in {config_path}"
                )

            params = dict(profiles[connection_name])
            params.pop("authenticator", None)
            params["authenticator"] = "username_password_mfa"
            params["password"] = getpass.getpass("Snowflake password: ")
            return Session.builder.configs(params).create()

        return Session.builder.config("connection_name", connection_name).create()

    required = {
        "account": "SNOWFLAKE_ACCOUNT",
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",
        "role": "SNOWFLAKE_ROLE",
    }
    missing = [env for env in required.values() if not os.getenv(env)]
    if missing:
        raise ValueError(
            "Missing Snowflake connection values: " + ", ".join(missing)
        )

    params: Dict[str, str] = {
        key: os.environ[env_name] for key, env_name in required.items()
    }
    params["warehouse"] = os.getenv("SNOWFLAKE_WAREHOUSE", WAREHOUSE)
    params["database"] = os.getenv("SNOWFLAKE_DATABASE", DATABASE)
    params["schema"] = os.getenv("SNOWFLAKE_SCHEMA", "ANALYTICS")
    return Session.builder.configs(params).create()


def build_customer_rfm(session: Session):
    """Calculate RFM metrics, quintile scores and a readable segment."""
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
    """Aggregate monthly order trends and calculate month-over-month growth."""
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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run TPC-H Snowpark analytics")
    parser.add_argument(
        "--connection",
        default="default",
        help="Name in ~/.snowflake/connections.toml; pass an empty string to use env vars",
    )
    parser.add_argument(
        "--password-auth",
        action="store_true",
        help="Prompt securely for a password instead of using external-browser SSO",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    session = create_session(args.connection, args.password_auth)
    try:
        session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()
        session.sql(f"USE DATABASE {DATABASE}").collect()

        rfm = build_customer_rfm(session)
        print(f"RFM completed: {rfm.count():,} customers")
        rfm.select(
            "C_CUSTKEY",
            "C_NAME",
            "RECENCY_DAYS",
            "FREQUENCY",
            "MONETARY",
            "RFM_SCORE",
            "RFM_SEGMENT",
        ).sort(F.col("MONETARY").desc()).show(10)

        trend = analyze_sales_trend(session)
        print(f"Sales trend completed: {trend.count():,} months")
        trend.show(12)

        # The assignment explicitly asks for a Pandas conversion so the
        # monthly result can be handed to a visualization tool.  The trend
        # table contains only one row per month (80 rows for TPC-H SF1), so it
        # is safe to collect locally after all aggregation is complete.
        trend_pandas = trend.to_pandas()
        print(f"Pandas trend rows: {len(trend_pandas):,}")
        print(trend_pandas.head(10).to_string(index=False))
    finally:
        session.close()


if __name__ == "__main__":
    main()

