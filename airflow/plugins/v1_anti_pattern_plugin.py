import logging
import os
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl
from airflow.utils.state import TaskInstanceState
from airflow.models.taskinstance import TaskInstance

from prometheus_client import CollectorRegistry, Counter, pushadd_to_gateway

log = logging.getLogger(__name__)

"""
V1 Anti-Pattern Plugin: Counter-based Pushgateway Metrics
=========================================================
This plugin demonstrates TWO compounding anti-patterns when using Prometheus
Pushgateway for batch workload observability.

ANTI-PATTERN 1 — Missing Run Isolation (Race Condition):
  Without run_id in the grouping key, all parallel task instances for the same
  task write to the same Pushgateway group. The Pushgateway is a stateless
  cache — it does not aggregate values, it blindly overwrites the previous
  payload. If 10 tasks fail simultaneously, only the last push is retained.
  You lose 9 failure records to a silent race condition.

  This race condition is caused by the GROUPING KEY design, NOT the metric type.
  It applies equally to Counters AND Gauges.

ANTI-PATTERN 2 — Using Counters for Task State:
  Even if you fix the race condition by adding run_id, Counters are
  semantically wrong for batch workload metrics:

  1. Retry Corruption: If a task fails (task_status_total{state="failed"}++)
     then retries and succeeds (task_status_total{state="success"}++), your
     dashboard shows BOTH a failure AND a success for the same task instance.
     The failure count is permanently inflated.

  2. No Final State: Counters can only accumulate events — they cannot model
     state transitions. You end up with separate counters for every state,
     and no clean way to query "what was this task's FINAL outcome?"

IMPORTANT — CARDINALITY IS NOT SOLVED BY METRIC TYPE:
  High cardinality is determined by LABELS, not metric type. Both Counters and
  Gauges produce identical cardinality when labelled with run_id. Switching to
  Gauges fixes semantic correctness (retry safety, state modelling) — it does
  NOT reduce cardinality or eliminate the need for a Sweeper DAG.

  See v2_gauge_fix_plugin.py for the corrected approach using Gauges.

Configuration (via environment variables):
•  AIRFLOW_VAR_ENABLE_V1_COUNTERS: Activate this plugin (default: 'false')
•  AIRFLOW_VAR_PUSHGATEWAY_URL: Push Gateway URL
•  AIRFLOW_VAR_PROMETHEUS_METRICS_ENABLED: Master on/off switch (default: 'true')
•  AIRFLOW_VAR_PROMETHEUS_PUSH_TIMEOUT_SECONDS: HTTP timeout (default: '5')
•  AIRFLOW_VAR_PROMETHEUS_INSTANCE_NAME: Label for Airflow instance (default: 'airflow-local')

Metric Exported (1 total):
•  task_status_total (Counter): Count of task state transitions, labelled by state=(success|failed)
"""


class PushgatewayV1CounterListeners:
    def __init__(self):
        self.instance_name = os.environ.get("AIRFLOW_VAR_PROMETHEUS_INSTANCE_NAME", "airflow-local")

    def _should_run(self):
        return os.environ.get("AIRFLOW_VAR_ENABLE_V1_COUNTERS", "false").lower() == "true"

    def _get_task_group_key(self, ti):
        """ANTI-PATTERN (V1): Coarse key — omits run_id so all parallel task
        instances for the same task overwrite the same Pushgateway group."""
        return {
            'dag_id': ti.dag_id,
            'task_id': ti.task_id,
            'instance': self.instance_name,
        }
        # THE FIX: include run_id so every task instance gets its own isolated
        # Pushgateway group. Fixes the race condition but introduces high
        # cardinality — every execution creates a new time series that grows
        # unboundedly without a Sweeper DAG.
        # return {
        #     'dag_id': ti.dag_id,
        #     'task_id': ti.task_id,
        #     'run_id': ti.run_id,
        #     'instance': self.instance_name,
        # }

    def _push(self, registry, group_key):
        if os.environ.get("AIRFLOW_VAR_PROMETHEUS_METRICS_ENABLED", "true").lower() != "true":
            return

        push_gateway_url = os.environ.get("AIRFLOW_VAR_PUSHGATEWAY_URL", "http://pushgateway:9091")
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
            log.info(f"[V1] Pushed task_status_total to Pushgateway {push_gateway_url}")
        except Exception as e:
            log.error(f"[V1] Failed to push metric to Pushgateway: {e}")

    @hookimpl
    def on_task_instance_success(self, previous_state: TaskInstanceState, task_instance: TaskInstance):
        if not self._should_run():
            return
        registry = CollectorRegistry()
        group_key = self._get_task_group_key(task_instance)

        # ANTI-PATTERN: Counter increments permanently. If this task previously
        # failed and retried, task_status_total{state="failed"} still exists
        # in this group — the failure is never erased.
        c = Counter('task_status_total', 'Count of task state transitions', ['state'], registry=registry)
        c.labels(state='success').inc()

        self._push(registry, group_key)

    @hookimpl
    def on_task_instance_failed(self, previous_state: TaskInstanceState, task_instance: TaskInstance, error):
        if not self._should_run():
            return
        registry = CollectorRegistry()
        group_key = self._get_task_group_key(task_instance)

        c = Counter('task_status_total', 'Count of task state transitions', ['state'], registry=registry)
        c.labels(state='failed').inc()

        self._push(registry, group_key)


class V1AntiPatternPlugin(AirflowPlugin):
    name = "v1_anti_pattern_plugin"
    listeners = [PushgatewayV1CounterListeners()]
