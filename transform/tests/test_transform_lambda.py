import pytest, os, boto3
from moto import mock_aws
from unittest.mock import patch

# Patch environment variables for both S3 buckets
# with patch.dict(os.environ, {"ingestion_zone_bucket": "test_ingestion_bucket", "processed_data_zone_bucket": "test_processed_bucket"}):
#     from transform.src.transform_lambda import (
#         connect_to_db,
#         lambda_handler,
#         write_data,
#         read_history_data_from_any_tb,
#         read_updates_from_any_tb,
#     )

# Add fixtures to mock AWS connection and create two S3 test buckets

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture
def test_ingestion_bucket(s3):
    s3.create_bucket(
        Bucket="test_ingestion_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("transform/tests/test_data/counterparty-14_56_53.298826.json") as f:
        text_to_write = f.read()
        s3.put_object(
            Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/counterparty-14_56_53.298826.json"
        )


@pytest.fixture
def test_processed_bucket(s3):
    s3.create_bucket(
        Bucket="test_processed_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

# Add tests from util functions

# Add tests for writing to the processed data bucket

@pytest.mark.describe('Transform lambda handler tests')
class TestTransfomLambdaHandler:

    @pytest.mark.it('Does not execute if ingestion bucket is empty')
    def test_transform_lambda_empty_ingestion_bucket(self, s3):
        pass