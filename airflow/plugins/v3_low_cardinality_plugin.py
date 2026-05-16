# Bare-bones Pushgateway Plugin - v3_low_cardinality_plugin
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl

log = logging.getLogger(__name__)

class V3LowCardinalityPlugin(AirflowPlugin):
    name = "v3_low_cardinality_plugin"
