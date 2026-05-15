"""
Demo ETL Job — No Spark Required
=================================
Simulates the same Extract → Transform → Load pipeline as etl_job.py
but uses plain Python instead of PySpark. Pushes identical metrics to
the Pushgateway so you can demo the full observability flow without
a Spark cluster.

Run:
    python spark/demo_etl_job.py
    python spark/demo_etl_job.py --pushgateway-url http://localhost:9091
    python spark/demo_etl_job.py --simulate-failure

Requires:
    pip install prometheus-client
"""

import argparse
import logging
import random
import time

from pushgateway_metrics import SparkMetricsReporter

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("demo_etl_job")


def extract() -> list[dict]:
    """Generate synthetic sensor readings."""
    log.info("EXTRACT: generating 100,000 synthetic sensor records")
    data = [
        {
            "sensor_id": i,
            "temperature": random.uniform(-10, 70),
            "humidity": random.uniform(10, 100),
        }
        for i in range(100_000)
    ]
    time.sleep(1.5)  # simulate I/O latency
    return data


def transform(data: list[dict]) -> list[dict]:
    """Filter anomalous readings, add derived columns."""
    log.info("TRANSFORM: filtering & enriching data")
    clean = [
        {
            **row,
            "heat_index": row["temperature"] * 1.8 + 32 + row["humidity"] * 0.1,
            "quality": (
                "cold" if row["temperature"] < 10
                else "normal" if row["temperature"] < 35
                else "hot"
            ),
        }
        for row in data
        if 0 <= row["temperature"] <= 55 and 20 <= row["humidity"] <= 95
    ]
    time.sleep(2)  # simulate compute
    return clean


def load(data: list[dict]) -> int:
    """Simulate writing to storage."""
    count = len(data)
    log.info("LOAD: writing %d records (simulated)", count)
    time.sleep(1)  # simulate write
    return count


def main():
    parser = argparse.ArgumentParser(description="Demo ETL with Pushgateway metrics (no Spark)")
    parser.add_argument(
        "--pushgateway-url",
        default=None,
        help="Prometheus Pushgateway URL (default: env PUSHGATEWAY_URL or http://localhost:9091)",
    )
    parser.add_argument(
        "--simulate-failure",
        action="store_true",
        help="Simulate a failure during the transform stage",
    )
    args = parser.parse_args()

    reporter = SparkMetricsReporter(
        app_name="sensor-etl-demo",
        app_id=f"demo-{int(time.time())}",
        pushgateway_url=args.pushgateway_url,
    )

    reporter.report_job_start()
    status = "success"
    total_records = 0

    try:
        # ----- Extract -----
        with reporter.timed_stage("extract") as ctx:
            raw_data = extract()
            ctx["records_out"] = len(raw_data)
            log.info("Extracted %d records", len(raw_data))

        # ----- Transform -----
        with reporter.timed_stage("transform") as ctx:
            ctx["records_in"] = len(raw_data)
            if args.simulate_failure:
                raise RuntimeError("Simulated transform failure: data schema mismatch")
            clean_data = transform(raw_data)
            ctx["records_out"] = len(clean_data)
            log.info("Transformed: %d → %d records", len(raw_data), len(clean_data))

        # ----- Load -----
        with reporter.timed_stage("load") as ctx:
            ctx["records_in"] = len(clean_data)
            total_records = load(clean_data)
            ctx["records_out"] = total_records

    except Exception:
        status = "failed"
        log.exception("ETL job failed")
    finally:
        reporter.report_job_end(status=status, total_records=total_records)
        log.info("Job finished with status=%s", status)


if __name__ == "__main__":
    main()
