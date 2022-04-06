"""Entry point for ETL worker"""
from typing import AsyncIterable, Dict, List
import logging.config

from etl.config import settings
from etl.database.database import PGDatabase
from etl.event_processor import GeneralEventProcessor, EtlConfigEventProcessor
from etl.messaging.kafka_producer import KafkaMessageProducer
from etl.object_store.minio import MinioObjectStore
from etl.tasking.faust import FaustTaskSink, FaustAppConfig

logging.config.fileConfig(settings.logging_conf_file)

faust_app_configs: List[FaustAppConfig] = []

message_producer = KafkaMessageProducer()
object_store = MinioObjectStore()
database = PGDatabase()

etl_config_event_processor = EtlConfigEventProcessor(object_store=object_store)

etl_source_data_event_processor = GeneralEventProcessor(object_store=object_store,
                                                        message_producer=message_producer,
                                                        database=database)

#Faust Agent, or Stream Processor definitions https://faust.readthedocs.io/en/latest/userguide/agents.html#what-is-an-agent
#Faust Agent definition to process ETL Toml Config
async def etl_config_file_evt(evts: AsyncIterable[Dict]) -> None:
    async for evt in evts:
        etl_config_event_processor.process(evt)

#Fause Agent definition to process ETL source data files
async def general_file_evt(evts: AsyncIterable[Dict]) -> None:
    async for evt in evts:
        etl_source_data_event_processor.process(evt)


# configure two separate Faust Apps for the single Faust worker in FaustTaskSink
# config for Faust App that handles the ETL TOML config bucket notification event
faust_app_configs.append(FaustAppConfig(kafka_topic=settings.kafka_topic_castiron_etl_config,
                                        kafka_consumer_grp=settings.consumer_grp_etl_config,
                                        app_agent_func=etl_config_file_evt))

# config for Faust App that handles the ETL source data file bucket notification event
faust_app_configs.append(FaustAppConfig(kafka_topic=settings.kafka_topic_castiron_etl_source_file,
                                        kafka_consumer_grp=settings.consumer_grp_etl_source_file,
                                        app_agent_func=general_file_evt))

etl_config_sink: FaustTaskSink = FaustTaskSink(faust_app_configs=faust_app_configs)

etl_config_sink.start()
