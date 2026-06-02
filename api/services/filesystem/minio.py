import asyncio
import io
import os
from typing import Any, BinaryIO, Dict, Optional

import boto3
from botocore.client import Config
from loguru import logger
from minio import Minio
from minio.error import S3Error

from .base import BaseFileSystem


class MinioFileSystem(BaseFileSystem):
    """MinIO implementation of the filesystem interface for OSS users.

    Two endpoints, two different purposes:
    - endpoint (host:port) + secure (bool): used by the MinIO SDK for
      container-to-container calls. The SDK requires these split.
    - public_endpoint (full URL, e.g. "https://example.com"): used verbatim
      when building URLs that browsers will fetch. Required.
    """

    def __init__(
        self,
        endpoint: str = "localhost:9000",
        access_key: str = "minioadmin",
        secret_key: str = "minioadmin",
        bucket_name: str = "voice-audio",
        secure: bool = False,
        public_endpoint: Optional[str] = None,
    ):
        if not public_endpoint:
            raise ValueError(
                "MinioFileSystem requires public_endpoint (set MINIO_PUBLIC_ENDPOINT). "
                "Expected a full URL with scheme, e.g. 'http://localhost:9000' or 'https://example.com'."
            )
        if not (
            public_endpoint.startswith("http://")
            or public_endpoint.startswith("https://")
        ):
            raise ValueError(
                f"MINIO_PUBLIC_ENDPOINT must include a scheme (http:// or https://), got: {public_endpoint!r}"
            )

        self.bucket_name = bucket_name
        self.endpoint = endpoint
        self.public_endpoint = public_endpoint.rstrip("/")
        self.secure = secure
        self.access_key = access_key
        self.secret_key = secret_key
        # Region must match the MinIO server's configured region.
        # Override via MINIO_REGION env var (default matches standard MinIO default).
        self.region = os.getenv("MINIO_REGION", "us-east-1")

        # Client for internal operations (uploads, downloads, etc.)
        # region is passed explicitly to prevent the SDK from calling
        # GetBucketLocation, which fails for restricted service accounts.
        self.client = Minio(
            endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure,
            region=self.region,
        )

        # Ensure bucket exists (using internal client)
        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except Exception as e:
            logger.debug(f"Bucket setup note: {e}")

    async def acreate_file(self, file_path: str, content: BinaryIO) -> bool:
        try:
            data = await content.read()

            def _put():
                self.client.put_object(
                    self.bucket_name,
                    file_path,
                    data=io.BytesIO(data),
                    length=len(data),
                )

            await asyncio.to_thread(_put)
            return True
        except S3Error:
            return False

    async def aupload_file(self, local_path: str, destination_path: str) -> bool:
        try:

            def _fput():
                self.client.fput_object(self.bucket_name, destination_path, local_path)

            await asyncio.to_thread(_fput)
            return True
        except S3Error:
            return False

    async def aget_signed_url(
        self,
        file_path: str,
        expiration: int = 3600,
        force_inline: bool = False,
        use_internal_endpoint: bool = False,
    ) -> Optional[str]:
        try:
            if use_internal_endpoint:
                protocol = "https" if self.secure else "http"
                base = f"{protocol}://{self.endpoint}"
            else:
                base = self.public_endpoint
            return f"{base}/{self.bucket_name}/{file_path}"
        except Exception as e:
            logger.error(f"Error generating MinIO URL: {e}")
            return None

    async def aget_file_metadata(self, file_path: str) -> Optional[Dict[str, Any]]:
        """Get MinIO object metadata."""
        try:

            def _stat():
                return self.client.stat_object(self.bucket_name, file_path)

            stat = await asyncio.to_thread(_stat)
            return {
                "size": stat.size,
                "created_at": stat.last_modified,
                "modified_at": stat.last_modified,
                "etag": stat.etag.strip('"') if stat.etag else None,
                "content_type": stat.content_type,
                "storage_class": None,  # MinIO doesn't have storage classes like S3
            }
        except S3Error:
            return None

    async def aget_presigned_put_url(
        self,
        file_path: str,
        expiration: int = 900,
        content_type: str = "text/csv",
        max_size: int = 10_485_760,
    ) -> Optional[str]:
        """Generate a presigned PUT URL for direct browser-to-storage upload.

        Uses boto3 to generate the signature locally (no API calls to MinIO),
        signed against the public endpoint so the URL is usable from browsers.
        The region must match the MinIO server's configured region (MINIO_REGION).
        """
        try:
            s3 = boto3.client(
                "s3",
                endpoint_url=self.public_endpoint,
                aws_access_key_id=self.access_key,
                aws_secret_access_key=self.secret_key,
                config=Config(signature_version="s3v4"),
                region_name=self.region,
            )
            url = s3.generate_presigned_url(
                "put_object",
                Params={"Bucket": self.bucket_name, "Key": file_path},
                ExpiresIn=expiration,
            )
            logger.debug(f"Generated presigned PUT URL for {file_path}")
            return url
        except Exception as e:
            logger.error(f"Error generating presigned upload URL: {e}")
            return None

    async def adownload_file(self, source_path: str, local_path: str) -> bool:
        """Download a file from MinIO to local path."""
        try:

            def _fget():
                self.client.fget_object(self.bucket_name, source_path, local_path)

            await asyncio.to_thread(_fget)
            return True
        except S3Error:
            return False

    async def acopy_file(self, source_path: str, destination_path: str) -> bool:
        """Copy a file within MinIO (server-side copy)."""
        try:
            from minio.commonconfig import CopySource

            def _copy():
                self.client.copy_object(
                    self.bucket_name,
                    destination_path,
                    CopySource(self.bucket_name, source_path),
                )

            await asyncio.to_thread(_copy)
            return True
        except S3Error:
            return False
