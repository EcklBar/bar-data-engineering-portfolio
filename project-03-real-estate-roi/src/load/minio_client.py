"""
Minio S3-Compatible Storage Client
מודול לאחסון נתונים ב-Minio (תואם S3)

Handles partitioned storage for raw and processed data.
"""

import io
import json
import logging
from datetime import datetime
from typing import Optional, Union
import os

from minio import Minio
from minio.error import S3Error
import pandas as pd

logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_BUCKET = "real-estate-data"
DEFAULT_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
DEFAULT_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
DEFAULT_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")


class MinioStorageClient:
    """Client for storing and retrieving data from Minio S3-compatible storage."""

    def __init__(
        self,
        endpoint: str = DEFAULT_ENDPOINT,
        access_key: str = DEFAULT_ACCESS_KEY,
        secret_key: str = DEFAULT_SECRET_KEY,
        secure: bool = False,
        bucket: str = DEFAULT_BUCKET
    ):
        """
        Initialize Minio client.

        Args:
            endpoint: Minio server endpoint (host:port)
            access_key: Access key (username)
            secret_key: Secret key (password)
            secure: Use HTTPS
            bucket: Default bucket name
        """
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self.default_bucket = bucket
        self._ensure_bucket_exists(bucket)

    def _ensure_bucket_exists(self, bucket: str) -> None:
        """Create bucket if it doesn't exist."""
        try:
            if not self.client.bucket_exists(bucket):
                self.client.make_bucket(bucket)
                logger.info(f"Created bucket: {bucket}")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    def _generate_partitioned_path(
        self,
        base_path: str,
        date: Optional[datetime] = None,
        partition_by: str = "date"
    ) -> str:
        """
        Generate a partitioned path based on date.

        Args:
            base_path: Base path (e.g., "raw/gov_transactions")
            date: Date for partitioning (defaults to today)
            partition_by: Partition scheme ("date", "month", "year")

        Returns:
            Partitioned path like "raw/gov_transactions/year=2024/month=01/day=15/"
        """
        date = date or datetime.now()

        if partition_by == "date":
            partition = f"year={date.year}/month={date.month:02d}/day={date.day:02d}"
        elif partition_by == "month":
            partition = f"year={date.year}/month={date.month:02d}"
        elif partition_by == "year":
            partition = f"year={date.year}"
        else:
            partition = ""

        return f"{base_path}/{partition}".rstrip("/")

    def upload_json(
        self,
        data: Union[dict, list],
        path: str,
        bucket: Optional[str] = None,
        partitioned: bool = True,
        partition_date: Optional[datetime] = None
    ) -> str:
        """
        Upload JSON data to Minio.

        Args:
            data: Dictionary or list to upload as JSON
            path: Base path (e.g., "raw/gov_transactions/data.json")
            bucket: Bucket name (uses default if not specified)
            partitioned: Whether to add date partitions to path
            partition_date: Date for partitioning

        Returns:
            Full object path where data was stored
        """
        bucket = bucket or self.default_bucket

        if partitioned:
            # Split path into base and filename
            path_parts = path.rsplit("/", 1)
            if len(path_parts) == 2:
                base_path, filename = path_parts
            else:
                base_path, filename = "", path_parts[0]

            partitioned_base = self._generate_partitioned_path(base_path, partition_date)
            object_name = f"{partitioned_base}/{filename}"
        else:
            object_name = path

        # Convert to JSON bytes
        json_bytes = json.dumps(data, ensure_ascii=False, indent=2, default=str).encode("utf-8")
        data_stream = io.BytesIO(json_bytes)

        try:
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=data_stream,
                length=len(json_bytes),
                content_type="application/json"
            )
            logger.info(f"Uploaded JSON to: {bucket}/{object_name}")
            return f"{bucket}/{object_name}"

        except S3Error as e:
            logger.error(f"Failed to upload JSON: {e}")
            raise

    def upload_parquet(
        self,
        df: pd.DataFrame,
        path: str,
        bucket: Optional[str] = None,
        partitioned: bool = True,
        partition_date: Optional[datetime] = None
    ) -> str:
        """
        Upload DataFrame as Parquet to Minio.

        Args:
            df: Pandas DataFrame to upload
            path: Base path (e.g., "processed/transactions/data.parquet")
            bucket: Bucket name
            partitioned: Whether to add date partitions
            partition_date: Date for partitioning

        Returns:
            Full object path where data was stored
        """
        bucket = bucket or self.default_bucket

        if partitioned:
            path_parts = path.rsplit("/", 1)
            if len(path_parts) == 2:
                base_path, filename = path_parts
            else:
                base_path, filename = "", path_parts[0]

            partitioned_base = self._generate_partitioned_path(base_path, partition_date)
            object_name = f"{partitioned_base}/{filename}"
        else:
            object_name = path

        # Convert DataFrame to Parquet bytes
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow")
        parquet_buffer.seek(0)

        try:
            self.client.put_object(
                bucket_name=bucket,
                object_name=object_name,
                data=parquet_buffer,
                length=parquet_buffer.getbuffer().nbytes,
                content_type="application/octet-stream"
            )
            logger.info(f"Uploaded Parquet to: {bucket}/{object_name} ({len(df)} rows)")
            return f"{bucket}/{object_name}"

        except S3Error as e:
            logger.error(f"Failed to upload Parquet: {e}")
            raise

    def download_json(
        self,
        path: str,
        bucket: Optional[str] = None
    ) -> Union[dict, list]:
        """
        Download JSON data from Minio.

        Args:
            path: Object path
            bucket: Bucket name

        Returns:
            Parsed JSON data
        """
        bucket = bucket or self.default_bucket

        try:
            response = self.client.get_object(bucket, path)
            data = json.loads(response.read().decode("utf-8"))
            response.close()
            response.release_conn()
            return data

        except S3Error as e:
            logger.error(f"Failed to download JSON: {e}")
            raise

    def download_parquet(
        self,
        path: str,
        bucket: Optional[str] = None
    ) -> pd.DataFrame:
        """
        Download Parquet file as DataFrame.

        Args:
            path: Object path
            bucket: Bucket name

        Returns:
            Pandas DataFrame
        """
        bucket = bucket or self.default_bucket

        try:
            response = self.client.get_object(bucket, path)
            parquet_buffer = io.BytesIO(response.read())
            response.close()
            response.release_conn()

            return pd.read_parquet(parquet_buffer)

        except S3Error as e:
            logger.error(f"Failed to download Parquet: {e}")
            raise

    def list_objects(
        self,
        prefix: str = "",
        bucket: Optional[str] = None,
        recursive: bool = True
    ) -> list:
        """
        List objects in bucket with optional prefix filter.

        Args:
            prefix: Path prefix to filter
            bucket: Bucket name
            recursive: List recursively

        Returns:
            List of object names
        """
        bucket = bucket or self.default_bucket

        objects = self.client.list_objects(
            bucket_name=bucket,
            prefix=prefix,
            recursive=recursive
        )

        return [obj.object_name for obj in objects]

    def object_exists(self, path: str, bucket: Optional[str] = None) -> bool:
        """Check if an object exists."""
        bucket = bucket or self.default_bucket
        try:
            self.client.stat_object(bucket, path)
            return True
        except S3Error:
            return False


# Convenience functions for Airflow tasks
def save_raw_transactions(records: list, date: Optional[datetime] = None) -> str:
    """Save raw Gov.il transactions to Minio."""
    client = MinioStorageClient()
    return client.upload_json(
        data=records,
        path="raw/gov_transactions/transactions.json",
        partition_date=date
    )


def save_processed_transactions(df: pd.DataFrame, date: Optional[datetime] = None) -> str:
    """Save processed transactions as Parquet."""
    client = MinioStorageClient()
    return client.upload_parquet(
        df=df,
        path="processed/transactions/transactions.parquet",
        partition_date=date
    )


# Example usage
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Test connection
    client = MinioStorageClient()

    # Test JSON upload
    test_data = {
        "test": "data",
        "timestamp": datetime.now().isoformat(),
        "records": [{"id": 1, "value": "test"}]
    }

    path = client.upload_json(
        data=test_data,
        path="raw/test/test_data.json"
    )
    print(f"Uploaded to: {path}")

    # List objects
    objects = client.list_objects(prefix="raw/")
    print(f"Objects in raw/: {objects}")
