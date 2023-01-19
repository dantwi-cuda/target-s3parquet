"""s3parquet target class."""

from __future__ import annotations

from singer_sdk.target_base import Target
from singer_sdk import typing as th

from target_s3parquet.sinks import (
    s3parquetSink,
)


class Targets3parquet(Target):
    """Sample target for s3parquet."""

    name = "target-s3parquet"
    config_jsonschema = th.PropertiesList(
        th.Property(
            "s3_bucket",
            th.StringType,
            description="The S3 path were files will be stored (for file e.g. s3://bucket/foldername/ or s3://bucket/) "
        ),
        th.Property("s3_key_prefix", 
        th.StringType,
        description=""
        ),
        th.Property(
            "aws_region",
            th.StringType,
            secret=False,  # Flag config as protected.
            description="The path to the target output file"
        ),
        th.Property(
            "aws_secret_access_key",
            th.StringType,
            description="AWS Secret access key"
        ),
        th.Property(
            "aws_access_key_id",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="AWS Access Key ID"
        ),
        th.Property("aws_session_token", th.StringType),
        th.Property("aws_profile", th.StringType),
        th.Property(
            "stringify_schema",
            th.StringType,
            secret=True,  # Flag config as protected.
            description="If True all data types will be converted to string"
        ),
        th.Property(
            "athena_database",
            th.StringType,
            secret=False,  # Flag config as protected.
            default="default",
            description="Name of Athena database that will be used to store the metadata. Defaults to default database "
        ),
        th.Property(
            "compression",
            th.StringType,
            secret=False,  # Flag config as protected.
            default="zstd",
            description="Compression style (None, snappy, gzip, zstd) "
        ),
        th.Property(
            "add_record_metadata",
            th.StringType,
            secret=False,  # Flag config as protected.
            description="Add the meta data of records that will be created"
        ),
        th.Property(
            "partition_info",
            th.StringType,
            secret=False,  # Flag config as protected.
            description="The column that contains timestamp, datetime or date"
        ),
   
        

    ).to_dict()

    default_sink_class = s3parquetSink

cli=Targets3parquet.cli

