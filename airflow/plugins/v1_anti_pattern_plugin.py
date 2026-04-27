# Bare-bones Pushgateway Plugin - v1_anti_pattern_plugin
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl

log = logging.getLogger(__name__)

class V1AntiPatternPlugin(AirflowPlugin):
    name = "v1_anti_pattern_plugin"
