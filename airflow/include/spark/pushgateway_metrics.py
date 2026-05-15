"""
Prometheus Pushgateway Metrics for Spark Jobs
==============================================
Reusable helper that mirrors the Gauge-based V2 pattern used in the Airflow
pushgateway plugin. All metrics are Gauges pushed via pushadd_to_gateway so
that concurrent Spark jobs writing to the same Pushgateway don't clobber each
other's data.

Grouping key structure:
    job       = "spark_jobs"
    app_name  = <spark.app.name>
    app_id    = <spark.app.id>
    instance  = <configurable, default "spark-local">

Usage:
    from pushgateway_metrics import SparkMetricsReporter
    reporter = SparkMetricsReporter(pushgateway_url="http://host.docker.internal:9091")
    reporter.report_job_start()
    # ... do work ...
    reporter.report_stage("extract", duration_seconds=12.3, records_in=50000, records_out=48000)
    reporter.report_job_end(status="success", total_records=48000)

Environment variables (override defaults):
    PUSHGATEWAY_URL              - Pushgateway URL (default: http://host.docker.internal:9091)
    PROMETHEUS_PUSH_TIMEOUT      - HTTP timeout in seconds (default: 5)
    SPARK_METRICS_INSTANCE_NAME  - Instance label (default: spark-local)
"""

import logging
import os
import time
from contextlib import contextmanager

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

log = logging.getLogger(__name__)

JOB_NAME = "spark_jobs"


class SparkMetricsReporter:
    """Push Gauge-based metrics to Prometheus Pushgateway for a Spark job."""

    def __init__(
        self,
        app_name: str,
        app_id: str | None = None,
        pushgateway_url: str | None = None,
        instance_name: str | None = None,
        timeout: int | None = None,
    ):
        self.app_name = app_name
        self.app_id = app_id or f"{app_name}_{int(time.time())}"
        self.pushgateway_url = (
            pushgateway_url
            or os.environ.get("AIRFLOW_VAR_PUSHGATEWAY_URL")
            or os.environ.get("PUSHGATEWAY_URL", "http://host.docker.internal:9091")
        )
        self.instance_name = (
            instance_name
            or os.environ.get("SPARK_METRICS_INSTANCE_NAME", "spark-local")
        )
        self.timeout = timeout or int(
            os.environ.get("PROMETHEUS_PUSH_TIMEOUT", "5")
        )
        self._job_start_ts: float | None = None

    # ------------------------------------------------------------------
    # Grouping keys
    # ------------------------------------------------------------------
    def _job_group_key(self) -> dict[str, str]:
        return {
            "app_name": self.app_name,
            "app_id": self.app_id,
            "instance": self.instance_name,
        }

    def _stage_group_key(self, stage_name: str) -> dict[str, str]:
        return {
            **self._job_group_key(),
            "stage": stage_name,
        }

    # ------------------------------------------------------------------
    # Push helper (mirrors _push from Airflow V2 plugin)
    # ------------------------------------------------------------------
    def _push(self, registry: CollectorRegistry, group_key: dict[str, str]) -> None:
        # Use a unique job name including app_name and app_id to ensure distinct groups in Pushgateway
        full_job_name = f"{JOB_NAME}_{self.app_name}_{self.app_id}"
        try:
            pushadd_to_gateway(
                self.pushgateway_url,
                job=full_job_name,
                grouping_key=group_key,
                registry=registry,
                timeout=self.timeout,
            )
            log.info(
                "Pushed metrics to %s | group_key=%s",
                self.pushgateway_url,
                group_key,
            )
        except Exception:
            log.exception("Failed to push metrics to Pushgateway")

    # ------------------------------------------------------------------
    # Job-level metrics
    # ------------------------------------------------------------------
    def report_job_start(self) -> None:
        """Push a status=0 (running) gauge when the job begins."""
        self._job_start_ts = time.time()
        registry = CollectorRegistry()

        Gauge(
            "spark_job_start_timestamp",
            "Unix timestamp when Spark job started",
            registry=registry,
        ).set(self._job_start_ts)

        Gauge(
            "spark_job_status",
            "Job status snapshot (0=running, 1=success, -1=failed)",
            registry=registry,
        ).set(0)

        self._push(registry, self._job_group_key())

    def report_job_end(
        self,
        status: str = "success",
        total_records: int | None = None,
    ) -> None:
        """Push final job metrics: status, duration, optional record count."""
        registry = CollectorRegistry()
        group_key = self._job_group_key()

        status_val = 1 if status == "success" else -1
        Gauge(
            "spark_job_status",
            "Job status snapshot (0=running, 1=success, -1=failed)",
            registry=registry,
        ).set(status_val)

        if self._job_start_ts is not None:
            duration = time.time() - self._job_start_ts
            Gauge(
                "spark_job_duration_seconds",
                "Total job wall-clock duration",
                registry=registry,
            ).set(duration)

        if total_records is not None:
            Gauge(
                "spark_job_records_total",
                "Total records processed by the job",
                registry=registry,
            ).set(total_records)

        self._push(registry, group_key)

    # ------------------------------------------------------------------
    # Stage-level metrics
    # ------------------------------------------------------------------
    def report_stage(
        self,
        stage_name: str,
        duration_seconds: float,
        records_in: int | None = None,
        records_out: int | None = None,
    ) -> None:
        """Push metrics for a single logical stage (extract, transform, load)."""
        registry = CollectorRegistry()
        group_key = self._stage_group_key(stage_name)

        Gauge(
            "spark_stage_duration_seconds",
            "Duration of a logical stage",
            registry=registry,
        ).set(duration_seconds)

        if records_in is not None:
            Gauge(
                "spark_stage_records_in",
                "Records read during stage",
                registry=registry,
            ).set(records_in)

        if records_out is not None:
            Gauge(
                "spark_stage_records_out",
                "Records written during stage",
                registry=registry,
            ).set(records_out)

        Gauge(
            "spark_stage_status",
            "Stage status (1=success, -1=failed)",
            registry=registry,
        ).set(1)

        self._push(registry, group_key)

    # ------------------------------------------------------------------
    # Context manager for timed stages
    # ------------------------------------------------------------------
    @contextmanager
    def timed_stage(self, stage_name: str):
        """Context manager that times a stage and pushes metrics on exit.

        Usage:
            with reporter.timed_stage("transform") as ctx:
                df = df.filter(...)
                ctx["records_out"] = df.count()
        """
        ctx: dict = {"records_in": None, "records_out": None}
        start = time.time()
        try:
            yield ctx
            duration = time.time() - start
            self.report_stage(
                stage_name,
                duration_seconds=duration,
                records_in=ctx.get("records_in"),
                records_out=ctx.get("records_out"),
            )
        except Exception:
            # Push failure metric for the stage, then re-raise
            duration = time.time() - start
            registry = CollectorRegistry()
            group_key = self._stage_group_key(stage_name)
            Gauge(
                "spark_stage_duration_seconds",
                "Duration of a logical stage",
                registry=registry,
            ).set(duration)
            Gauge(
                "spark_stage_status",
                "Stage status (1=success, -1=failed)",
                registry=registry,
            ).set(-1)
            self._push(registry, group_key)
            raise
