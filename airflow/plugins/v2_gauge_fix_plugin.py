# Bare-bones Pushgateway Plugin - v2_gauge_fix_plugin
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl

log = logging.getLogger(__name__)

class V2GaugeFixPlugin(AirflowPlugin):
    name = "v2_gauge_fix_plugin"
