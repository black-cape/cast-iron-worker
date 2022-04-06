"""Contains the Minio implementation of the object store backend interface"""
from typing import Any, Dict, List, Optional

from asyncpg import Pool
from asyncpg_utils.databases import PoolDatabase
from asyncpg_utils.managers import TableManager

from etl.config import settings
from etl.database.interfaces import DatabaseStore


class PGDatabase(DatabaseStore):
    """Implements the DatabaseStore interface using Minio as the backend service"""
    def __init__(self):
        self._database = PoolDatabase(f'postgres://{settings.database_user}:{settings.database_password}@{settings.database_host}/{settings.database_db}')
        self._table_manager = TableManager(self._database, 'files', pk_field='id', hooks=None)

    async def db_pool(self) -> Pool:
        self._database.init_pool()

    async def insert_file(self, filedata: dict) -> None:
        await self._database.insert('files', filedata)

    async def move_file(self, id: str, newName: str) -> None:
        rec_data = {}
        rec_data['path'] = newName
        await self._table_manager.update(id, rec_data)

    async def delete_file(self, id: str) -> None:
        await self._table_manager.delete(id)

    async def list_files(self, metadata: Optional[dict]) -> List[Dict]:
        return await self._table_manager.list(filters=metadata)
        
    async def retrieve_file_metadata(self, id: str) -> dict:
        return await self._table_manager.detail(id)

    def parse_notification(self, evt_data: Any) -> Dict:
        bucket_name, file_name = evt_data['Key'].split('/', 1)
        db_evt = {
            'bucket_name': bucket_name,
            'file_name': file_name,
            'event_name': evt_data['EventName'],
            'source_ip': evt_data['Records'][0]['requestParameters']['sourceIpAddress'],
            'size': evt_data['Records'][0]['object']['size'],
            'etag': evt_data['Records'][0]['object']['eTag'],
            'content_type': evt_data['Records'][0]['object']['contentType'],
            'classification': evt_data['Records'][0]['userMetadata']['X-Amz-Meta-Classification']
        }
        return db_evt
