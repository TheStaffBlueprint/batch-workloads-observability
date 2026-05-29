import logging
import os
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstance import TaskInstance

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

log = logging.getLogger(__name__)

"""
V2 Gauge-based Pushgateway Plugin (Stepping Stone for Small Systems)
=====================================================================
This plugin corrects the two anti-patterns in v1_anti_pattern_plugin.py.

FIX 1 — Run Isolation via Grouping Key:
  run_id is added to the grouping key so every task instance gets its own
  isolated slot in the Pushgateway. Parallel tasks no longer overwrite each
  other. The race condition is eliminated.

  NOTE: This fix applies equally to Counters and Gauges. The race condition
  in V1 is caused by the missing run_id, NOT the metric type.

FIX 2 — Gauge for Semantic Correctness:
  task_status is a Gauge that represents the task's CURRENT state as an
  absolute value:
    1  = success
   -1  = failed

  Why Gauges are semantically correct for batch workloads:

  Retry Safety: If a task fails (task_status = -1) then retries and succeeds,
  the Gauge is overwritten to task_status = 1. Only the FINAL state is
  reflected. With Counters, both events persist — the failure count is
  permanently inflated.

WHAT GAUGES DO NOT SOLVE:
  Gauges do NOT reduce cardinality compared to Counters with the same labels.
  Both produce identical series counts in Prometheus. The Gauge advantage is
  purely semantic — modelling state vs. counting events.

⚠️  CARDINALITY WARNING:
  V2 injects run_id into the grouping key, creating a unique metric group for
  EVERY task execution. Cardinality grows linearly with executions. This
  requires an aggressive Sweeper DAG to prevent Pushgateway OOM.

  Acceptable for small-to-moderate systems (hundreds of runs/day).
  For production systems at scale, use v3_low_cardinality_plugin.py instead.

Configuration (via environment variables):
•  AIRFLOW_VAR_ENABLE_V2_GAUGES: Activate this plugin (default: 'false')
•  AIRFLOW_VAR_PUSHGATEWAY_URL: Push Gateway URL
•  AIRFLOW_VAR_PROMETHEUS_METRICS_ENABLED: Master on/off switch (default: 'true')
•  AIRFLOW_VAR_PROMETHEUS_PUSH_TIMEOUT_SECONDS: HTTP timeout (default: '5')
•  AIRFLOW_VAR_PROMETHEUS_INSTANCE_NAME: Label for Airflow instance (default: 'airflow-local')

Metric Exported (1 total):
•  task_status (Gauge): Task state snapshot (1=success, -1=failed)
   Grouping key: dag_id + task_id + run_id + instance
"""


class PushgatewayV2GaugeListeners:
    def __init__(self):
        self.instance_name = os.environ.get("AIRFLOW_VAR_PROMETHEUS_INSTANCE_NAME", "airflow-local")

    def _should_run(self):
        return os.environ.get("AIRFLOW_VAR_ENABLE_V2_GAUGES", "false").lower() == "true"

    def _get_task_group_key(self, ti):
        """FIX: run_id gives every task instance its own isolated slot in the
        Pushgateway. Parallel tasks no longer race to overwrite the same key.
        Trade-off: cardinality grows unboundedly — a Sweeper DAG is required."""
        return {
            'dag_id': ti.dag_id,
            'task_id': ti.task_id,
            'run_id': ti.run_id,
            'instance': self.instance_name,
        }

    def _push(self, registry, group_key):
        if os.environ.get("AIRFLOW_VAR_PROMETHEUS_METRICS_ENABLED", "true").lower() != "true":
            return

        push_gateway_url = os.environ.get("AIRFLOW_VAR_PUSHGATEWAY_URL", "http://host.docker.internal:9091")
        if not push_gateway_url:
            log.warning("PUSHGATEWAY_URL not set. Skipping metric push.")
            return

        timeout = int(os.environ.get("AIRFLOW_VAR_PROMETHEUS_PUSH_TIMEOUT_SECONDS", "5"))

        try:
            pushadd_to_gateway(
                push_gateway_url,
                job="airflow_tasks",
                grouping_key=group_key,
                registry=registry,
                timeout=timeout
            )
            log.info(f"[V2] Pushed task_status to Pushgateway {push_gateway_url}")
        except Exception as e:
            log.error(f"[V2] Failed to push metric to Pushgateway: {e}")

    @hookimpl
    def on_task_instance_success(self, previous_state: TaskInstanceState, task_instance: TaskInstance):
        if not self._should_run():
            return
        registry = CollectorRegistry()
        group_key = self._get_task_group_key(task_instance)

        g = Gauge('task_status', 'Task state snapshot (1=success, -1=failed)', registry=registry)
        g.set(1)  # 1 = success

        self._push(registry, group_key)

    @hookimpl
    def on_task_instance_failed(self, previous_state: TaskInstanceState, task_instance: TaskInstance, error):
        if not self._should_run():
            return
        registry = CollectorRegistry()
        group_key = self._get_task_group_key(task_instance)

        g = Gauge('task_status', 'Task state snapshot (1=success, -1=failed)', registry=registry)
        g.set(-1)  # -1 = failed

        self._push(registry, group_key)


class V2GaugeFixPlugin(AirflowPlugin):
    name = "v2_gauge_fix_plugin"
    listeners = [PushgatewayV2GaugeListeners()]
