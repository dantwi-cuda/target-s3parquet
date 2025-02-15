import os
from logging import Logger
import awswrangler as wr
import boto3
#from pyathena import connect


def create_session(config, logger: Logger):
    """Generates an athena client object

    Args:
        config ([type]): [description]
        logger (Logger): [description]

    Returns:
        cursor: athena client object
    """

    logger.info("Attempting to create BOTO3 session")

    # Get the required parameters from config file and/or environment variables
    aws_access_key_id = config.get("aws_access_key_id") or os.environ.get(
        "AWS_ACCESS_KEY_ID"
    )
    aws_secret_access_key = config.get("aws_secret_access_key") or os.environ.get(
        "AWS_SECRET_ACCESS_KEY"
    )
    aws_session_token = config.get("aws_session_token") or os.environ.get(
        "AWS_SESSION_TOKEN"
    )
    aws_profile = config.get("aws_profile") or os.environ.get("AWS_PROFILE")
    aws_region = config.get("aws_region") or os.environ.get("AWS_REGION")
    #s3_staging_dir = config.get("s3_staging_dir") or os.environ.get("S3_STAGING_DIR")

    aws_session =""

    # AWS credentials based authentication
    if aws_access_key_id and aws_secret_access_key:
        aws_session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
    elif aws_access_key_id and aws_secret_access_key and aws_session_token:
        aws_session = boto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=aws_region
        )
    # AWS Profile based authentication
    else:
        aws_ession = boto3.Session(
            profile_name=aws_profile,
            region_name=aws_region
        )
    logger.info("Ussing the follwoing Session Information: {aws_session}")    
    return aws_session


def create_database(aws_session,
    database: str="default"):
    
    if database not in wr.catalog.databases(boto3_session=aws_session).values:
        wr.catalog.create_database(database,boto3_session=aws_session)


