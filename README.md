# Batch Workloads Observability Architecture

This repository is the companion code for the blog series: **[Airflow Batch Observability: The Green Tick Fallacy & Pushgateway Trap]**. 

It provides a complete, production-ready, local observability stack demonstrating how to correctly extract granular metrics from Airflow batch workloads. The architecture uses StatsD for operational counts, Pushgateway with Gauges for task state snapshots, and includes automated cleanup via a Sweeper DAG.

### The Interactive Lab: Counter vs Gauge Semantics
This repository contains a built-in simulation to demonstrate why Prometheus Counters are semantically wrong for batch task state tracking.

We have two separate feature-flagged plugins in the `airflow/plugins/` directory:
1. **V1 Anti-Pattern (`v1_anti_pattern_plugin.py`):** Uses `Counter` metrics with a coarse grouping key (no `run_id`). This demonstrates two compounding problems: race conditions from missing run isolation, AND semantic incorrectness from using Counters for task state.
2. **V2 Gauge Fix (`v2_gauge_fix_plugin.py`):** Uses `Gauge` metrics with a granular grouping key (includes `run_id`). This fixes both problems: run isolation prevents overwrites, and Gauges correctly model task state with retry safety.

**What this lab proves:**
- The race condition (parallel tasks overwriting each other) is a **grouping key** problem, not a metric type problem. Adding `run_id` fixes it for both Counters and Gauges.
- The semantic incorrectness (inflated failure counts on retry) is a **metric type** problem. Only Gauges handle retries correctly by overwriting the previous state.
- Cardinality is **identical** for Counters and Gauges with the same labels. Gauges do not reduce cardinality or improve query performance.

**To run the simulation:**
1. Configure your `.env` (see below).
2. Enable V1 by setting `AIRFLOW_VAR_ENABLE_V1_COUNTERS=true` and restarting Airflow.
3. In the Airflow UI, trigger the `race_condition_simulator` DAG.
4. This DAG spins up 10 tasks that fail at the exact same millisecond. 
5. Check Grafana (Lab Dashboard). Because V1 uses a coarse grouping key without `run_id`, the Pushgateway overwrites all but the last push. You will see only 1 failure recorded instead of 10.
6. Now, disable V1, enable V2 (`AIRFLOW_VAR_ENABLE_V2_GAUGES=true`), and run the DAG again. Watch Grafana correctly show 10 failures, each as an isolated state snapshot.

### Environment Configuration (.env)
Create a `.env` file in the root of the `airflow/` directory. Copy and paste the following configuration:

```bash
# Pushgateway URL (Astro CLI requires host.docker.internal to reach the host)
AIRFLOW_VAR_PUSHGATEWAY_URL=http://host.docker.internal:9091

# Interactive Lab Flags (Enable one at a time)
AIRFLOW_VAR_ENABLE_V1_COUNTERS=true
AIRFLOW_VAR_ENABLE_V2_GAUGES=false
AIRFLOW_VAR_ENABLE_V3_LOW_CARDINALITY=false

# StatsD — Native Airflow metrics (Standard Production Architecture)
AIRFLOW__METRICS__STATSD_ON=True
AIRFLOW__METRICS__STATSD_HOST=host.docker.internal
AIRFLOW__METRICS__STATSD_PORT=8125
AIRFLOW__METRICS__STATSD_PREFIX=af_agg
```

## What's Inside?

1. **V1 Anti-Pattern Plugin** (`airflow/plugins/v1_anti_pattern_plugin.py`): Counters + coarse grouping key (no `run_id`). Demonstrates the race condition and semantic mismatch. **Do not use in production.**

2. **V2 Stepping Stone Plugin** (`airflow/plugins/v2_gauge_fix_plugin.py`): Gauges + `run_id` in grouping key. Fixes semantics and run isolation, but creates high cardinality. Requires a Sweeper DAG. **Acceptable for small systems only (≤ hundreds of runs/day).**

3. **V3 Production Plugin** (`airflow/plugins/v3_low_cardinality_plugin.py`): Gauges without `run_id`. Low, bounded cardinality. No Sweeper needed. Shows the latest state of each task. **Recommended for production.**

4. **The Sweeper DAG** (`airflow/dags/pushgateway_sweeper.py`): Required ONLY when using V2. Runs on a schedule, queries the Pushgateway REST API, and deletes stale metric groups.

5. **The Observability Stack** (`docker-compose.yml`): Prometheus, Pushgateway, StatsD Exporter, and Grafana, pre-configured to scrape all sources.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/)
- [Astro CLI](https://docs.astronomer.io/astro/cli/install-cli) (For running the local Airflow environment)

## Quick Start

### 1. Start the Observability Stack
First, spin up Prometheus, Pushgateway, and Grafana:

```bash
docker-compose up -d
```
*Wait a few seconds for the services to become healthy.*

You can now access:
- **Pushgateway UI:** http://localhost:9091
- **Prometheus UI:** http://localhost:9090
- **Grafana UI:** http://localhost:3000

### 2. Configure Airflow (.env)
Create the `.env` file in the `airflow/` directory using the template provided in the **Environment Configuration** section above.

### 3. Start Airflow
This project uses the Astronomer CLI to run Airflow locally.

```bash
astro dev start
```
*This will spin up the Airflow Webserver, Scheduler, and Database.*

You can access the Airflow UI at: http://localhost:8080 (Default credentials: `admin` / `admin`).

## How to Test the Architecture

1. **Generate some metrics:**
   Go to the Airflow UI (http://localhost:8080) and manually trigger the `example_dag`. Let it run to completion.

3. **Verify the Dashboards:**
   Go to Grafana (http://localhost:3000) and check the "Airflow Batch Workloads" folder.
   - **Airflow Observability (V2):** Use this to drill down into specific `run_id` history (best for small systems).
   - **Airflow V3 Low Cardinality:** Use this for a production-grade view of the latest state of every task across your entire fleet.

4. **Test the Sweeper DAG (V2 Only):**
   To see the self-cleaning mechanism in action when using V2, trigger the `pushgateway_sweeper` DAG. You can provide a custom runtime parameter (e.g., `{"max_age_mins": 1}`) to force it to delete the metrics you just generated instantly. Check the Pushgateway UI again — the stale metrics will be gone.

## ⚠️ The Architecture Warning (StatsD vs. Pushgateway vs. OLAP)

**Important:** This repository demonstrates how to use the Pushgateway to capture state snapshots using Gauges. You must adhere to strict metric boundaries to prevent infrastructure failures:

1. **For Global Counts & Timers (Low Cardinality):** Use Airflow's native StatsD exporter (`[metrics] statsd_on = True`). It aggregates UDP bursts in memory without race conditions or overwrites.

2. **For State Snapshots (Low Cardinality Labels Only):** Use the Pushgateway with Gauges only if you need multi-dimensional state tracking (e.g., tracking task states by DAG and task name). The **V3 plugin** (`v3_low_cardinality_plugin.py`) is the recommended approach — it shows the latest state of each task without generating unbounded cardinality.

3. **🚨 The Golden Rule:** NEVER inject high-cardinality keys like `run_id` or `task_instance_id` into Prometheus via StatsD or Pushgateway. Doing so causes severe series churn, bloats the Prometheus TSDB index, and destroys query performance. The **V2 plugin** demonstrates this approach as a stepping stone for small systems (hundreds of runs/day), but it is NOT production-grade at scale.

4. **Need to track exact metrics per `run_id`?** (rows processed, data quality, lineage) — **Stop using metrics.** Emit structured JSON logs to an OLAP engine (like ClickHouse or BigQuery) or a log aggregator (like Grafana Loki). Per-execution audit data is NOT a metrics problem.

### Plugin Evolution

| Plugin | Approach | Cardinality | Sweeper Needed | Use Case |
|---|---|---|---|---|
| **V1** (Anti-Pattern) | Counters, no `run_id` | Low | No | ❌ Race conditions + semantic mismatch |
| **V2** (Stepping Stone) | Gauges, with `run_id` | High (unbounded) | **Yes** | ⚠️ Small systems only (≤ hundreds of runs/day) |
| **V3** (Production) | Gauges, no `run_id` | Low (bounded) | No | ✅ Production-grade, latest state dashboard |

