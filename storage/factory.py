from __future__ import annotations

from config.settings import Settings
from storage.backend import StorageBackend
from storage.local import LocalStorageBackend
from storage.s3 import S3StorageBackend


def create_storage_backend(settings: Settings) -> StorageBackend:
    if settings.STORAGE_BACKEND == "azure_blob":
        from storage.azure_blob import AzureBlobStorageBackend

        return AzureBlobStorageBackend(
            connection_string=settings.AZURE_STORAGE_CONNECTION_STRING,
            container_name=settings.AZURE_STORAGE_CONTAINER,
        )
    if settings.STORAGE_BACKEND == "s3":
        return S3StorageBackend(
            bucket=settings.S3_BUCKET,
            endpoint_url=settings.S3_ENDPOINT_URL or None,
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID or None,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY or None,
            region_name=settings.AWS_REGION,
        )
    return LocalStorageBackend(base_dir=settings.STORAGE_LOCAL_DIR)
