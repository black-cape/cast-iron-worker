"""Describes interface for sending messages to a message broker"""
import abc
from typing import Any, Dict, List, Optional


class DatabaseStore(abc.ABC):
    """Interface for message producer backend"""

    async def insert_file(self, filedata: dict) -> None:
        """Insert a file record
        :param file: Dict containing record
        """
        raise NotImplementedError

    async def move_file(self, rowid: str, new_name: str) -> None:
        """Rename a file record
        :param id: The id
        :param newName: New path value
        """
        raise NotImplementedError

    async def update_status(self, rowid: str, new_status: str, new_filename: str) -> None:
        """Rename a file record
        :param id: The id
        :param newStatus: New status value
        """
        raise NotImplementedError

    async def delete_file(self, rowid: str) -> None:
        """Delete a record
        :param id: The id
        """
        raise NotImplementedError

    async def list_files(self, metadata: Optional[dict]) -> List[Dict]:
        """Retrieve records based metadata criteria
        :param metadata: Dict containing query restrictions
        """
        raise NotImplementedError

    async def retrieve_file_metadata(self, rowid: str) -> dict:
        """Retrieve a row based on ID
        :param id: The id
        """
        raise NotImplementedError

    def parse_notification(self, evt_data: Any) -> Dict:
        """Parse the event into a DB row/dict
        :param evt_data: The event data from S3/Minio
        """
        raise NotImplementedError
