"""Contains the Faust implementation of the TaskSink backend interface"""
import logging
from dataclasses import dataclass
from typing import Callable
from typing import Dict, Optional, List

import faust

from etl.config import settings
from etl.tasking.interfaces import TaskSink

LOGGER = logging.getLogger(__name__)


@dataclass
class FaustAppConfig:
    # minimal config needed for a simplistic Faust App that has a single call back function for all messages
    kafka_consumer_grp: str
    kafka_topic: str
    app_agent_func: Callable


class FaustTaskSink(TaskSink):
    """Implementation of TaskSink using Faust"""

    def __init__(self, faust_app_configs: List[FaustAppConfig]):
        """
            :param kafka_consumer_grp: Kafka Consumer Group ID to be used as App ID of Faust, see
            https://faust.readthedocs.io/en/latest/userguide/settings.html#std:setting-id
            :param kafka_topic: the kafka topic from which to consumer
        """
        self._event_callback: Callable[[Dict], None] = None

        self._apps: List[faust.App] = []
        for app_config in faust_app_configs:
            app = faust.App(
                app_config.kafka_consumer_grp,
                broker=f'kafka://{settings.kafka_broker}',
                value_serializer='raw'
            )

            # otherwise each app needs own port, we also don't need it to start a web server
            app.conf.web_enabled = False
            faust_topic: faust.TopicT = app.topic(app_config.kafka_topic, value_serializer='json')
            app.agent(faust_topic)(app_config.app_agent_func)

            self._apps.append(app)

        self._worker: Optional[faust.Worker] = None

    def start(self) -> None:
        self._worker = faust.Worker(*self._apps, loglevel='info')
        self._worker.execute_from_commandline()
