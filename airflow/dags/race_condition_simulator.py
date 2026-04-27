from airflow.decorators import dag, task
from datetime import datetime
import time

@dag(
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["simulation"],
    max_active_tasks=10
)
def race_condition_simulator():
    """
    This DAG simulates a highly parallel batch workload where multiple tasks fail
    at the exact same millisecond. 
    
    If AIRFLOW_VAR_ENABLE_V1_COUNTERS=true, the Pushgateway will drop 9 out of 10
    of these failures due to the Counter overwrite race condition.
    """

    # Generate 10 identical tasks that sleep briefly to align, then fail simultaneously
    @task(task_id="simulate_parallel_failure")
    def fail_simultaneous(item: int):
        # Brief sleep so they all align perfectly in the executor
        time.sleep(1)
        raise Exception(f"Simulated simultaneous failure for task array index {item}")

    # Map the task across an array of 10 items to run them in parallel
    fail_simultaneous.expand(item=list(range(10)))

dag_instance = race_condition_simulator()
