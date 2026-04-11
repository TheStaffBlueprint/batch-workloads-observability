from datetime import datetime, timedelta
import os
import logging
import urllib.parse
import time
import requests

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param

log = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'pushgateway_sweeper',
    default_args=default_args,
    description='A DAG to sweep old metrics from Prometheus Pushgateway to prevent memory leaks',
    schedule='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['observability', 'maintenance'],
    params={
        "max_age_mins": Param(default=1, type="integer", description="Maximum age of metrics to sweep, in minutes (Overrides manual runs only)"),
    }
) as dag:

    @task
    def clean_pushgateway(**kwargs):
        """
        Connects to the Pushgateway API, reads all metric groups, and sends a DELETE 
        request to wipe any group that hasn't been updated in `max_age_mins`.
        """
        dag_run = kwargs.get('dag_run')
        
        # Enforce 1440 min (24 hours) for automated schedules. Default to params for manual.
        if dag_run and dag_run.run_type == 'scheduled':
            max_age_mins = 1440
            log.info("Automated Scheduled Run detected: Enforcing max_age_mins = 1440")
        else:
            max_age_mins = kwargs.get('params', {}).get('max_age_mins', 1)
            log.info(f"Manual Run detected: Enforcing parameterized max_age_mins = {max_age_mins}")
        push_gateway_url = os.environ.get("AIRFLOW_VAR_PUSHGATEWAY_URL", "http://pushgateway:9091")
        log.info(f"Connecting to Pushgateway at {push_gateway_url}")
        
        # 1. Fetch current metrics
        try:
            response = requests.get(f"{push_gateway_url}/api/v1/metrics", timeout=10)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            log.error(f"Failed to fetch metrics: {e}")
            return
            
        now = time.time()
        max_age_seconds = max_age_mins * 60
        deleted_count = 0
        
        # 2. Iterate over all metric groups
        for group in data.get('data', []):
            try:
                # Find the push_time_seconds metric added by Pushgateway
                push_time = None
                if 'push_time_seconds' in group and 'metrics' in group['push_time_seconds']:
                    push_time = float(group['push_time_seconds']['metrics'][0]['value'])
                
                if push_time is None:
                    continue
                    
                age_seconds = now - push_time
                
                # If metric is older than threshold, delete it
                if age_seconds > max_age_seconds:
                    labels = group.get('labels', {})
                    if 'job' not in labels:
                        continue
                        
                    job = labels['job']
                    
                    # 3. Build the DELETE URL dynamically based on grouping keys
                    # URL format: /metrics/job/<job>/<label_name>/<label_value>...
                    delete_path = f"/metrics/job/{urllib.parse.quote(job, safe='')}"
                    
                    for key, val in labels.items():
                        if key != 'job' and val: # Pushgateway ignores empty string labels in grouping paths
                            delete_path += f"/{key}/{urllib.parse.quote(str(val), safe='')}"
                    
                    delete_url = f"{push_gateway_url}{delete_path}"
                    log.info(f"Deleting stale group (age {age_seconds/3600:.1f}h): {labels}")
                    
                    del_response = requests.delete(delete_url, timeout=10)
                    if del_response.status_code in [200, 202]:
                        deleted_count += 1
                        log.info(f"Successfully deleted.")
                    else:
                        log.warning(f"Failed to delete {delete_url}: HTTP {del_response.status_code} - {del_response.text}")
                        
            except Exception as e:
                log.error(f"Error processing group {group.get('labels')}: {e}")
                
        log.info(f"Sweeper finished successfully. Deleted {deleted_count} stale metric groups.")

    clean_pushgateway()
