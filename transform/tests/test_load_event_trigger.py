import json
from unittest.mock import patch
import pytest
import os
import boto3
from moto import mock_aws
import logging

# Patch environment variables for both S3 buckets
with patch.dict(os.environ, {"ingestion_zone_bucket": "test_ingestion_bucket", "processed_data_zone_bucket": "test_processed_bucket"}):
    from transform.src.processed_lambda import (
        lambda_handler
    )

logger = logging.getLogger("test")
logger.setLevel(logging.INFO)
logger.propagate = True


@pytest.fixture
def valid_event():
    with open("transform/tests/test_data_for_load/valid_test_event.json") as v:
        event = json.loads(v.read())
    return event


@pytest.fixture
def invalid_event():
    with open("transform/tests/test_data_for_load/invalid_test_event.json") as i:
        event = json.loads(i.read())
    return event


@pytest.fixture
def file_type_event():
    with open("transform/tests/test_data_for_load/file_type_event.json") as i:
        event = json.loads(i.read())
    return event


@pytest.fixture
def wrong_type_event():
    with open("transform/tests/test_data_for_load/wrong_type_event.json") as i:
        event = json.loads(i.read())
    return event


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
        yield boto3.client("s3", region_name="eu-west-1")


@pytest.fixture
def ingestion_bucket(s3):
    s3.create_bucket(
        Bucket="test_ingestion_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("transform/tests/test_data/sales_order-15_36_42.731009.json") as f:
        text_to_write = f.read()
        s3.put_object(
            Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/sales_order-15_36_42.731009.json"
        )
    with open("transform/tests/test_data_for_load/wrong.txt", "rb") as f:
        s3.put_object(Body=f, Bucket="test_ingestion_bucket", Key="sample/wrong.txt")
    

@pytest.fixture
def processed_bucket(s3):
    s3.create_bucket(
        Bucket="test_processed_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    with open("transform/tests/test_data/sales_order-15_36_42.731009.json") as f:
        text_to_write = f.read()
        s3.put_object(
            Body=text_to_write, Bucket="test_processed_bucket", Key="2024-05-21/sales_order-15_36_42.731009/historic.json"
        )

@pytest.mark.describe("Test lambda handler event trigger")
class TestLambdaEventTrigger:

    @pytest.mark.it("if there is a valid event and correct type put the object processed s3")
    def test_valid_event(self, s3, valid_event, processed_bucket, ingestion_bucket):
        lambda_handler(valid_event, {})
        response = s3.list_objects_v2(Bucket="test_processed_bucket")
        assert response["KeyCount"] == 2
        assert response['Contents'][0]['Key'] == '2024-05-21/sales_order-15_36_42.731009.parquet'

    @pytest.mark.it("lambda throws logs message if is not valid json type")
    def test_invalid_type(self, file_type_event, caplog, s3, processed_bucket, ingestion_bucket):
        with caplog.at_level(logging.ERROR):
            lambda_handler(file_type_event, {})
            assert "File is not a valid json file" in caplog.text

    @pytest.mark.it("if there is an invalid event returns exception error")
    def test_invalid_event(self, s3, invalid_event, caplog):
        with caplog.at_level(logging.ERROR):
            lambda_handler(invalid_event, {})
        assert "No such bucket" in caplog.text

