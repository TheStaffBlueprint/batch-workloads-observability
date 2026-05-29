import logging
import os
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstance import TaskInstance

from prometheus_client import CollectorRegistry, Gauge, pushadd_to_gateway

log = logging.getLogger(__name__)

"""
V3 Low-Cardinality Pushgateway Plugin (Production-Grade)
========================================================
This plugin is the production-recommended approach for tracking task state
in Prometheus via the Pushgateway.

THE EVOLUTION:
  V1 (Anti-Pattern): Counter + no run_id → race condition + semantic mismatch
  V2 (Stepping Stone): Gauge + run_id → fixes semantics and race condition,
     but creates unbounded cardinality. Requires a Sweeper DAG.
  V3 (Production): Gauge + NO run_id → bounded, static cardinality.
     The number of metric groups equals the number of unique (dag_id, task_id)
     pairs, which never grows. No Sweeper DAG required.

HOW V3 WORKS:
  The grouping key contains only dag_id + task_id. This means each task has
  exactly ONE slot in the Pushgateway. When a new execution completes, it
  overwrites the previous task_status value.

  The result is a "latest state" view:
    - "What is the CURRENT state of task X?" → answered by V3
    - "What happened in run Y specifically?" → NOT answered by V3 (use OLAP/logs)

  This is the correct trade-off for production. Prometheus is for operational
  monitoring ("is the system healthy right now?"), not per-execution audit.

TRADE-OFFS:
  ✅ Bounded cardinality — no series churn, no Sweeper needed
  ✅ Retry safe — Gauge overwrites reflect only the final state
  ❌ No per-run history — only the LATEST execution state is visible
  ❌ For full audit trails (rows processed, error details), emit structured
     JSON logs to an OLAP engine (ClickHouse) or log aggregator (Grafana Loki)

Configuration (via environment variables):
•  AIRFLOW_VAR_ENABLE_V3_LOW_CARDINALITY: Activate this plugin (default: 'false')
•  AIRFLOW_VAR_PUSHGATEWAY_URL: Push Gateway URL
•  AIRFLOW_VAR_PROMETHEUS_METRICS_ENABLED: Master on/off switch (default: 'true')
•  AIRFLOW_VAR_PROMETHEUS_PUSH_TIMEOUT_SECONDS: HTTP timeout (default: '5')
•  AIRFLOW_VAR_PROMETHEUS_INSTANCE_NAME: Label for Airflow instance (default: 'airflow-local')

Metric Exported (1 total):
•  task_status (Gauge): Task state snapshot (1=success, -1=failed)
   Grouping key: dag_id + task_id + instance (NO run_id — bounded cardinality)
"""


class PushgatewayV3LowCardinalityListeners:
    def __init__(self):
        self.instance_name = os.environ.get("AIRFLOW_VAR_PROMETHEUS_INSTANCE_NAME", "airflow-local")

    def _should_run(self):
        return os.environ.get("AIRFLOW_VAR_ENABLE_V3_LOW_CARDINALITY", "false").lower() == "true"

    def _get_task_group_key(self, ti):
        """LOW CARDINALITY: No run_id. Each (dag_id, task_id) pair has exactly
        ONE slot in the Pushgateway. Latest execution overwrites previous.
        Cardinality is bounded by the number of unique tasks — never grows."""
        return {
            'dag_id': ti.dag_id,
            'task_id': ti.task_id,
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
            log.info(f"[V3] Pushed task_status to Pushgateway {push_gateway_url}")
        except Exception as e:
            log.error(f"[V3] Failed to push metric to Pushgateway: {e}")

    @hookimpl
    def on_task_instance_success(self, previous_state: TaskInstanceState, task_instance: TaskInstance):
        if not self._should_run():
            return
        registry = CollectorRegistry()
        group_key = self._get_task_group_key(task_instance)

        g = Gauge('task_status', 'Task state snapshot (1=success, -1=failed)', registry=registry)
        g.set(1)  # 1 = success — overwrites any previous value for this task

        self._push(registry, group_key)

    @hookimpl
    def on_task_instance_failed(self, previous_state: TaskInstanceState, task_instance: TaskInstance, error):
        if not self._should_run():
            return
        registry = CollectorRegistry()
        group_key = self._get_task_group_key(task_instance)

        g = Gauge('task_status', 'Task state snapshot (1=success, -1=failed)', registry=registry)
        g.set(-1)  # -1 = failed — overwrites any previous value for this task

        self._push(registry, group_key)


class V3LowCardinalityPlugin(AirflowPlugin):
    name = "v3_low_cardinality_plugin"
    listeners = [PushgatewayV3LowCardinalityListeners()]
