# Bare-bones Pushgateway Plugin - pushgateway_listener_plugin
import logging
from airflow.plugins_manager import AirflowPlugin
from airflow.listeners import hookimpl

log = logging.getLogger(__name__)

class PushgatewayListenerPlugin(AirflowPlugin):
    name = "pushgateway_listener_plugin"
