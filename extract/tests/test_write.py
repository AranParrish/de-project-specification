import pytest
import os
from moto import mock_aws
import boto3
import logging
from unittest.mock import Mock, patch

with patch.dict(os.environ, {"ingestion_zone_bucket": "test_bucket"}):
    from extract.src.write import write_data

@pytest.fixture(scope='function')
def mock_aws_credentials():

    """Mocked AWS Credentials for moto."""

    os.environ['AWS_ACCESS_KEY_ID'] = 'test'
    os.environ['AWS_SECRET_ACCESS_KEY'] = 'test'
    os.environ['AWS_SECURITY_TOKEN'] = 'test'
    os.environ['AWS_SESSION_TOKEN'] = 'test'
    os.environ['AWS_DEFAULT_REGION'] = 'eu-west-2'

@pytest.fixture(scope='function')
def s3(mock_aws_credentials):

    """Yield mocked boto3 's3' client"""

    with mock_aws():
        yield boto3.client('s3', region_name='eu-west-2')

@pytest.fixture(scope='function')
def bucket(s3):
    
    """Create test s3 bucket for writing data"""

    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )


@pytest.mark.describe('Write data input tests')
class TestWriteDataInputs:

    @pytest.mark.it('Input is not mutated')
    def test_write_data_input_not_mutated(self, s3):
        test_input = [1, 2, 3]
        copy_test_input = [1, 2, 3]
        write_data(s3, test_input)
        assert test_input == copy_test_input


@pytest.mark.describe('Write data S3 bucket tests')
class TestWriteDataToS3:

    @pytest.mark.it("Able to put file in S3 bucket")
    def test_write_to_s3(self, s3, bucket):
        
        data = [{"a": 1}, {"b": 2}]
        resp = write_data(s3, data)
        listing = s3.list_objects_v2(Bucket="test_bucket")
        assert len(listing["Contents"]) == 1
        assert '_totesys_snapshot' in listing["Contents"][0]["Key"]
        assert resp

    @pytest.mark.it("Logs client error if there is no S3 bucket")
    def test_write_s3_logs_client_error(self, s3, caplog):
        data = [{"a": 1}, {"b": 2}]
        with caplog.at_level(logging.INFO):
            resp = write_data(s3, data)
            assert not resp
            assert "ClientError" in caplog.text