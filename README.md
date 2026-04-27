# Batch Workloads Observability Architecture

This repository is the companion code for the article: **[Airflow Batch Observability: The Green Tick Fallacy & Pushgateway Trap]**. 

It provides a complete, production-ready, local observability stack demonstrating how to correctly extract granular metrics from Airflow batch workloads using the Prometheus Pushgateway, and how to safely manage Pushgateway state to avoid OOM crashes.

### The Interactive Lab: Simulating the Race Condition
This repository contains a built-in simulation to mathematically prove why you should not use Prometheus Pushgateway for Counters. 

We have two separate feature-flagged plugins in the `airflow/plugins/` directory:
1. **V1 Anti-Pattern:** Uses `Counter`.
2. **V2 Gauge Fix:** Uses `Gauge` for state snapshots.

**To run the simulation:**
1. Configure your `.env` (see below).
2. Enable V1 by setting `AIRFLOW_VAR_ENABLE_V1_COUNTERS=true` and restarting Airflow.
3. In the Airflow UI, trigger the `race_condition_simulator` DAG.
4. This DAG spins up 10 tasks that fail at the exact same millisecond. 
5. Check Grafana (Lab Dashboard). Because of the Pushgateway overwrite race condition, you will only see 1 failure recorded instead of 10. You just lost 9 metrics.
6. Now, disable V1, enable V2 (`AIRFLOW_VAR_ENABLE_V2_GAUGES=true`), and run the DAG again. Watch Grafana correctly show 10 failures.

### Environment Configuration (.env)
Create a `.env` file in the root of the `airflow/` directory. Copy and paste the following configuration:

```bash
# Pushgateway URL (Astro CLI requires host.docker.internal to reach the host)
AIRFLOW_VAR_PUSHGATEWAY_URL=http://host.docker.internal:9091

# Interactive Lab Flags (Enable one at a time)
AIRFLOW_VAR_ENABLE_V1_COUNTERS=true
AIRFLOW_VAR_ENABLE_V2_GAUGES=false

# StatsD — Native Airflow metrics (Standard Production Architecture)
AIRFLOW__METRICS__STATSD_ON=True
AIRFLOW__METRICS__STATSD_HOST=host.docker.internal
AIRFLOW__METRICS__STATSD_PORT=8125
AIRFLOW__METRICS__STATSD_PREFIX=airflow
```

## What's Inside?

1. **The Custom Airflow Plugins (`airflow/plugins/v1_anti_pattern_plugin.py` & `airflow/plugins/v2_gauge_fix_plugin.py`)**: 
   A strict Airflow 3 compliant plugin that listens to task lifecycle events (`on_task_instance_running`, `on_task_instance_success`, `on_task_instance_failed`). It pushes state to the Pushgateway using `Gauge` metrics, dynamically injecting the `run_id` to prevent parallel tasks from silently overwriting each other.
   
2. **The Sweeper DAG (`airflow/dags/pushgateway_sweeper.py`)**: 
   Because Pushgateway has no native TTL (Time To Live), pushing dynamic labels like `run_id` will eventually cause the Pushgateway to run out of memory. This DAG runs on a schedule, queries the Pushgateway REST API, and deletes stale metric groups older than 24 hours.

3. **The Observability Stack (`docker-compose.yml`)**:
   A lightweight local stack containing Prometheus, Pushgateway, and Grafana, pre-configured to scrape the gateway.

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

2. **Verify the Pushgateway:**
   Go to the Pushgateway UI (http://localhost:9091). You should see the custom Airflow metrics populated. Notice how they are grouped strictly by `dag_id`, `task_id`, `run_id`, and `instance` to prevent race conditions!

3. **Test the Sweeper DAG:**
   To see the self-cleaning mechanism in action, trigger the `pushgateway_sweeper` DAG. You can provide a custom runtime parameter (e.g., `{"max_age_mins": 1}`) to force it to delete the metrics you just generated instantly. Check the Pushgateway UI again—the stale metrics will be gone!

## The Architecture Warning (StatsD vs Pushgateway)

**Important:** This repository demonstrates how to use the Pushgateway to safely capture complex state snapshots. However, as discussed in the companion article, you should **never** use Pushgateway to track accumulative metrics (like total failure counts or exact runtimes). 

For counts and timers, you should enable Airflow's native **StatsD** exporter (`[metrics] statsd_on = True`), which safely accumulates UDP bursts without race conditions. Use Pushgateway *only* for metrics that require complex labels that StatsD struggles to parse.
