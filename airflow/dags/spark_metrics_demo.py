from airflow.decorators import dag
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import datetime
import os

@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "observability", "metrics"],
    doc_md="""
    ## Spark Metrics Observability Demo
    
    This DAG runs a Spark ETL job inside the Airflow container using `pyspark`.
    The Spark job is instrumented to emit metrics to the Prometheus Pushgateway.
    
    **Prerequisites:**
    - `pyspark` and `prometheus-client` installed in Airflow (via requirements.txt)
    - `openjdk-11-jre-headless` installed (via packages.txt)
    - Prometheus Pushgateway running in the same network
    """
)
def spark_metrics_demo():
    
    # In Astro, the project root is at /usr/local/airflow
    airflow_home = os.environ.get("AIRFLOW_HOME", "/usr/local/airflow")
    spark_dir = os.path.join(airflow_home, "include/spark")
    # print(f"DEBUG: Files in {spark_dir}: {os.listdir(spark_dir)}")
    spark_script = os.path.join(spark_dir, "etl_job.py")
    
    run_spark_job = BashOperator(
        task_id="run_spark_etl",
        bash_command=f"PYTHONPATH={spark_dir} python {spark_script}",
        append_env=True,
    )

spark_metrics_demo()
