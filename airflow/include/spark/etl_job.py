"""
Example Spark ETL Job with Prometheus Pushgateway Metrics
=========================================================
Demonstrates a simple Extract → Transform → Load pipeline that pushes
per-stage and per-job metrics to the Pushgateway using the same Gauge-based
pattern as the Airflow V2 plugin.

Submit:
    spark-submit \
        --master local[*] \
        --py-files pushgateway_metrics.py \
        etl_job.py \
        --pushgateway-url http://localhost:9091

Requires:
    pip install pyspark prometheus-client
"""

import argparse
import logging
import sys

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

from pushgateway_metrics import SparkMetricsReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("etl_job")


# -------------------------------------------------------------------------
# ETL steps
# -------------------------------------------------------------------------
def extract(spark: SparkSession) -> DataFrame:
    """Generate a synthetic dataset simulating sensor readings."""
    log.info("EXTRACT: generating synthetic sensor data")
    df = spark.range(0, 100_000).select(
        F.col("id").alias("sensor_id"),
        (F.rand() * 100).alias("temperature"),
        (F.rand() * 60 + 40).alias("humidity"),
        F.current_timestamp().alias("ts"),
    )
    return df


def transform(df: DataFrame) -> DataFrame:
    """Filter anomalous readings and add derived columns."""
    log.info("TRANSFORM: filtering & enriching data")
    df = (
        df.filter(
            (F.col("temperature").between(0, 55))
            & (F.col("humidity").between(20, 95))
        )
        .withColumn("heat_index", F.col("temperature") * 1.8 + 32 + F.col("humidity") * 0.1)
        .withColumn("quality", F.when(F.col("temperature") < 10, "cold")
                     .when(F.col("temperature") < 35, "normal")
                     .otherwise("hot"))
    )
    return df


def load(df: DataFrame, output_path: str) -> int:
    """Write results to parquet and return row count."""
    log.info("LOAD: writing results to %s", output_path)
    df.write.mode("overwrite").parquet(output_path)
    count = df.count()
    log.info("LOAD: wrote %d records", count)
    return count


# -------------------------------------------------------------------------
# Main
# -------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Spark ETL with Pushgateway metrics")
    parser.add_argument(
        "--pushgateway-url",
        default=None,
        help="Prometheus Pushgateway URL (default: env PUSHGATEWAY_URL or http://localhost:9091)",
    )
    parser.add_argument(
        "--output-path",
        default="/tmp/spark_etl_output",
        help="Parquet output path (default: /tmp/spark_etl_output)",
    )
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("sensor-etl-demo")
        .getOrCreate()
    )

    reporter = SparkMetricsReporter(
        app_name=spark.sparkContext.appName,
        app_id=spark.sparkContext.applicationId,
        pushgateway_url=args.pushgateway_url,
    )

    reporter.report_job_start()
    status = "success"
    total_records = 0

    try:
        # ----- Extract -----
        with reporter.timed_stage("extract") as ctx:
            raw_df = extract(spark)
            row_count = raw_df.count()
            ctx["records_out"] = row_count
            log.info("Extracted %d records", row_count)

        # ----- Transform -----
        with reporter.timed_stage("transform") as ctx:
            ctx["records_in"] = row_count
            clean_df = transform(raw_df)
            clean_count = clean_df.count()
            ctx["records_out"] = clean_count
            log.info("Transformed: %d → %d records", row_count, clean_count)

        # ----- Load -----
        with reporter.timed_stage("load") as ctx:
            ctx["records_in"] = clean_count
            total_records = load(clean_df, args.output_path)
            ctx["records_out"] = total_records

    except Exception:
        status = "failed"
        log.exception("ETL job failed")
        raise
    finally:
        reporter.report_job_end(status=status, total_records=total_records)
        spark.stop()


if __name__ == "__main__":
    main()
