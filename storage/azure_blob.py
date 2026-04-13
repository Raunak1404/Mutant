from __future__ import annotations

from datetime import datetime, timedelta, timezone

from azure.storage.blob.aio import BlobServiceClient
from azure.storage.blob import generate_blob_sas, BlobSasPermissions


class AzureBlobStorageBackend:
    """Azure Blob Storage backend for production deployment."""

    def __init__(self, connection_string: str, container_name: str) -> None:
        self.connection_string = connection_string
        self.container_name = container_name
        self._client = BlobServiceClient.from_connection_string(connection_string)

    async def close(self) -> None:
        """Close the underlying Azure SDK HTTP session."""
        await self._client.close()

    def _blob(self, key: str):
        return self._client.get_blob_client(container=self.container_name, blob=key)

    async def upload(self, key: str, data: bytes) -> None:
        blob = self._blob(key)
        await blob.upload_blob(data, overwrite=True)

    async def download(self, key: str) -> bytes:
        blob = self._blob(key)
        stream = await blob.download_blob()
        return await stream.readall()

    async def get_presigned_url(self, key: str, expires_in: int = 3600) -> str:
        # Parse account name and key from connection string for SAS generation
        account_name = self._client.account_name
        account_key = _extract_account_key(self.connection_string)

        sas_token = generate_blob_sas(
            account_name=account_name,
            container_name=self.container_name,
            blob_name=key,
            account_key=account_key,
            permission=BlobSasPermissions(read=True),
            expiry=datetime.now(timezone.utc) + timedelta(seconds=expires_in),
        )
        return f"https://{account_name}.blob.core.windows.net/{self.container_name}/{key}?{sas_token}"

    async def delete(self, key: str) -> None:
        blob = self._blob(key)
        await blob.delete_blob(delete_snapshots="include")

    async def exists(self, key: str) -> bool:
        blob = self._blob(key)
        try:
            await blob.get_blob_properties()
            return True
        except Exception:
            return False


def _extract_account_key(connection_string: str) -> str:
    """Extract AccountKey from an Azure Storage connection string."""
    for part in connection_string.split(";"):
        if part.strip().startswith("AccountKey="):
            return part.strip().split("=", 1)[1]
    raise ValueError("AccountKey not found in connection string")
