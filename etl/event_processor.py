"""Contains the implementation of the EventProcessor class"""
import json
import multiprocessing as mp
import os
import subprocess
import tempfile
import traceback
from pathlib import Path
from typing import Dict, Optional
from uuid import uuid4

from etl.config import settings
from etl.database.database import PGDatabase
from etl.file_processor_config import (FileProcessorConfig,
                                       load_python_processor, try_loads)
from etl.messaging.interfaces import MessageProducer
from etl.object_store.interfaces import EventType, ObjectStore
from etl.object_store.object_id import ObjectId
from etl.path_helpers import (filename, get_archive_path, get_error_path,
                              get_inbox_path, get_processing_path, parent,
                              processor_matches, rename)
from etl.pizza_tracker import PizzaTracker
from etl.util import create_rest_client, get_logger, short_uuid

LOGGER = get_logger(__name__)
ERROR_LOG_SUFFIX = '_error_log_.txt'
file_suffix_to_ignore = ['.toml', '.keep', ERROR_LOG_SUFFIX]


# As per stackoverflow.com needs to bubble exception up to parent
class ProcessWithExceptionBubbling(mp.Process):
    """ Class the handles multiprocessing with exception bubbling """
    def __init__(self, *args, **kwargs):
        mp.Process.__init__(self, *args, **kwargs)
        self._pconn, self._cconn = mp.Pipe()
        self._exception = None

    def run(self):
        try:
            mp.Process.run(self)
            self._cconn.send(None)
        except Exception as e:
            tb = traceback.format_exc()
            self._cconn.send((e, tb[0: 4000]))  # cap stack trace print to 4000
            # raise e  # You can still rise this exception if you need to

    @property
    def exception(self):
        """ Exception bubbling """
        if self._pconn.poll():
            self._exception = self._pconn.recv()
        return self._exception


class EtlConfigEventProcessor:
    """A service that processes individual object events"""
    # cached list of processor configs, needs to be accessed outside of ths class
    processors: Dict[ObjectId, FileProcessorConfig] = {}

    def __init__(self, object_store: ObjectStore):
        self._object_store = object_store

        # Load existing config files
        for obj in self._object_store.list_objects(settings.minio_etl_bucket, None, recursive=True):
            if obj.path.endswith('.toml'):
                self._toml_put(obj)

    def process(self, evt_data: Dict) -> None:
        """Object event process entry point"""
        evt = self._object_store.parse_notification(evt_data)

        if evt.object_id.path.endswith('.toml'):
            LOGGER.info(f'processing ETL processing Config file with path  {evt.object_id.path}')
            if evt.event_type == EventType.Delete:
                self._toml_delete(evt.object_id)
            elif evt.event_type == EventType.Put:
                self._toml_put(evt.object_id)
            LOGGER.info(f'finished ETL processing Config file with path  {evt.object_id.path}')

    def _toml_put(self, toml_object_id: ObjectId) -> bool:
        """Handle put event with TOML extension.
        :return: True if the operation is successful.
        """
        try:
            obj = self._object_store.read_object(toml_object_id)
            data: str = obj.decode('utf-8')
            cfg: FileProcessorConfig = try_loads(data)

            if cfg.enabled:
                # Register processor
                EtlConfigEventProcessor.processors[toml_object_id] = cfg
                LOGGER.info('number of processor configs: %s', len(EtlConfigEventProcessor.processors))
                for processor_key in EtlConfigEventProcessor.processors.keys():
                    LOGGER.info(processor_key)

                self._object_store.ensure_directory_exists(get_inbox_path(toml_object_id, cfg))
                self._object_store.ensure_directory_exists(get_processing_path(toml_object_id, cfg))
                self._object_store.ensure_directory_exists(get_error_path(toml_object_id, cfg))
                archive_object_id: Optional[ObjectId] = get_archive_path(toml_object_id, cfg)
                if archive_object_id:
                    self._object_store.ensure_directory_exists(get_archive_path(toml_object_id, cfg))
            return True
        except ValueError as exc:
            LOGGER.error(f'Failed to process toml {exc}')
            # Raised if we fail to parse and validate config
            return False

    def _toml_delete(self, toml_object_id: ObjectId) -> bool:
        """Handle remove event with TOML extension.
        :return: True if the object was deleted
        """
        return bool(EtlConfigEventProcessor.processors.pop(toml_object_id, None))


class GeneralEventProcessor:
    """A service that processes individual object events"""

    def __init__(self, object_store: ObjectStore, message_producer: MessageProducer):
        self._object_store = object_store
        self._message_producer = message_producer
        self._rest_client = create_rest_client()
        self._database = PGDatabase()
        self._database_active = False

    async def process(self, evt_data: Dict) -> None:
        """Object event process entry point"""

        # Check for database connection
        if not self._database_active:
            self._database_active = await self._database.create_table()
        db_evt = {}

        evt = self._object_store.parse_notification(evt_data)
        # this processor would get TOML config as well as regular file upload, there doesn't seem to be a way
        # to filter bucket notification to exclude by file path
        if any([evt.object_id.path.endswith(i) for i in file_suffix_to_ignore]):
            pass
        else:
            if evt.event_type == EventType.Delete:
                pass
            elif evt.event_type == EventType.Put:
                if evt.original_filename:
                    if not evt.event_status:
                        if self._database_active:
                            db_evt = self._database.parse_notification(evt_data)
                            try:
                                await self._database.insert_file(db_evt)
                            except Exception as e:
                                LOGGER.error(f'Database error.  Unable to process/track file.  Exception: {e}')
                        await self._file_put(evt.object_id, db_evt.get('id', None))
                else:
                    # New object. "Rename" object.
                    obj_path = Path(evt.object_id.path)
                    dirpath = obj_path.parent
                    filename = obj_path.name
                    obj_uuid = str(uuid4())
                    new_path = f'{dirpath}/{obj_uuid}-{filename}'
                    dest_object_id = ObjectId(evt.object_id.namespace, f'{new_path}')

                    metadata = evt_data['Records'][0]['s3']['object'].get('userMetadata', {})
                    metadata['originalFilename'] = filename
                    metadata['id'] = obj_uuid

                    self._object_store.move_object(evt.object_id, dest_object_id, metadata)




    async def _file_put(self, object_id: ObjectId, uuid: str) -> bool:
        """Handle possible data file puts.
        :return: True if successful.
        """
        # pylint: disable=too-many-locals,too-many-branches, too-many-statements
        LOGGER.info(f'received file put event for path {object_id}')
        LOGGER.info(f'number of ETL processing configs now {len(EtlConfigEventProcessor.processors)}')

        for config_object_id, processor in EtlConfigEventProcessor.processors.items():
            if (
                parent(object_id) != get_inbox_path(config_object_id, processor) or
                not processor_matches(
                    object_id, config_object_id, processor, self._object_store, self._rest_client,
                    settings.tika_host, settings.enable_tika
                )
            ):
                # File isn't in our inbox directory or filename doesn't match our glob pattern
                continue

            LOGGER.info(f'matching ETL processing config found, processing {config_object_id}')

            # Hypothetical file paths for each directory
            processing_file = get_processing_path(config_object_id, processor, object_id)
            archive_file: Optional[ObjectId] = get_archive_path(config_object_id, processor, object_id)
            error_file = get_error_path(config_object_id, processor, object_id)
            error_log_file_name = f'{filename(object_id).replace(".", "_")}{ERROR_LOG_SUFFIX}'
            error_log_file = get_error_path(config_object_id, processor, rename(object_id, error_log_file_name))

            job_id = short_uuid()
            self._message_producer.job_created(job_id, filename(object_id), filename(config_object_id), 'castiron')

            # mv to processing
            metadata = self._object_store.retrieve_object_metadata(object_id)
            metadata['status'] = 'Processing'
            self._object_store.move_object(object_id, processing_file, metadata)
            await self._database.update_status(uuid, 'Processing', processing_file.path)

            with tempfile.TemporaryDirectory() as work_dir:
                # Download to local temp working directory
                local_data_file = os.path.join(work_dir, filename(object_id))
                self._object_store.download_object(processing_file, local_data_file)

                with open(os.path.join(work_dir, 'out.txt'), 'w') as out, \
                        PizzaTracker(self._message_producer, work_dir, job_id) as pizza_tracker:

                    success = True

                    if processor.shell is not None:
                        env = {
                            'DATABASE_HOST': settings.database_host,
                            'DATABASE_PASSWORD': settings.database_password,
                            'DATABASE_PORT': str(settings.database_port),
                            'DATABASE_TABLE': str(settings.database_table),
                            'DATABASE_USER': str(settings.database_user),
                            'ETL_FILENAME': local_data_file,
                            'PIZZA_TRACKER': pizza_tracker.pipe_file_name,
                            'ETL_FILE_METADATA': json.dumps(metadata)
                        }

                        proc = subprocess.Popen(processor.shell,
                                                shell=True,
                                                executable='/bin/bash',
                                                env=env,
                                                stderr=subprocess.STDOUT,
                                                stdout=out if processor.save_error_log else subprocess.DEVNULL)

                        while True:
                            exit_code = None
                            try:
                                exit_code = proc.wait(.5)
                            except subprocess.TimeoutExpired:
                                pass

                            if exit_code is not None:
                                break

                            pizza_tracker.process()

                        success = exit_code == 0

                    elif processor.python is not None:
                        # pizza tracker has to be called in main thread as it needs things like Kafka connector
                        run_method = load_python_processor(processor.python)
                        method_kwargs = {}
                        if processor.python.supports_pizza_tracker:
                            method_kwargs['pizza_tracker'] = pizza_tracker.pipe_file_name
                            method_kwargs['pizza_job_id'] = job_id
                        if processor.python.supports_metadata:
                            method_kwargs['file_metadata'] = metadata

                        run_process = ProcessWithExceptionBubbling(
                            target=run_method,
                            args=(local_data_file,),
                            kwargs=method_kwargs
                        )
                        run_process.start()

                        # [WS] check once here to avoid spamming logs
                        if processor.python.supports_pizza_tracker:
                            LOGGER.debug('processor supports pizza tracker, will start tracker process')
                        else:
                            LOGGER.warning(f'processor {config_object_id} does not support pizza tracker')

                        while run_process.is_alive():
                            run_process.join(0.5)  # block up to 0.5 second each time waiting for processor completion
                            if run_process.exception:
                                LOGGER.error(
                                    f'Failed to process file, error will also be dumped to location specified '
                                    f'in ETL processing config if specified. Error was: {run_process.exception}')
                                out.write(
                                    'Failed to process '
                                    f'{processor.python.dict()} due to: \r\n {run_process.exception}'
                                )
                                out.flush()  # without flushing 0 byte gets moved to MINIO
                                success = False
                                break

                            if processor.python.supports_pizza_tracker:
                                pizza_tracker.process()

                    else:
                        LOGGER.error('No shell or python configuration set.')
                        return False

                    if success:
                        # Success. mv to archive
                        if archive_file:
                            metadata['status'] = 'Success'
                            self._object_store.move_object(processing_file, archive_file, metadata)
                        await self._database.update_status(uuid, 'Success', archive_file.path)
                        self._message_producer.job_evt_status(job_id, 'success')
                    else:
                        # Failure. mv to failed
                        metadata['status'] = 'Failed'
                        self._object_store.move_object(processing_file, error_file, metadata)
                        await self._database.update_status(uuid, 'Failed', error_file.path)
                        self._message_producer.job_evt_status(job_id, 'failure')

                        # Optionally save error log to failed, use same metadata as original file
                        if processor.save_error_log:
                            self._object_store.upload_object(dest=error_log_file,
                                                             src_file=os.path.join(work_dir, 'out.txt'),
                                                             metadata=metadata)

                # Success or not, we handled this
                LOGGER.info(f'finished processing {object_id}')
                return True

        # Not our table
        return False
