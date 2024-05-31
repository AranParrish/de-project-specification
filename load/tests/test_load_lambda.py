import pytest
from pg8000 import Connection, InterfaceError
import os
from moto import mock_aws
import boto3
import logging
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

with patch.dict(os.environ, {"processed_data_zone_bucket": "test_bucket"}):
    from load.src.load_lambda import (
        connect_to_db,
        get_dw_creds,
        DW_CREDS,
    )


@pytest.fixture(scope="function")
def mock_aws_credentials():
    """Mocked AWS Credentials for moto."""

    os.environ["AWS_ACCESS_KEY_ID"] = "test"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "test"
    os.environ["AWS_SECURITY_TOKEN"] = "test"
    os.environ["AWS_SESSION_TOKEN"] = "test"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"


@pytest.fixture(scope="function")
def s3(mock_aws_credentials):
    """Yield mocked boto3 's3' client"""

    with mock_aws():
        yield boto3.client("s3", region_name="eu-west-2")


@pytest.fixture(scope="function")
def bucket(s3):
    """Create test s3 bucket for writing data"""

    s3.create_bucket(
        Bucket="test_bucket",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield "test_bucket"


@pytest.mark.describe("Get DB credentials function tests")
class TestGetDwCreds:

    @pytest.mark.it("valid name test")
    def test_retrieve_secret_valid_name(self):
        secret_name = "dw_creds"
        region = "eu-west-2"
        assert get_dw_creds(secret_name, region) != {}

    @pytest.mark.it("invalid name test")
    def test_retrieve_secret_invalid_name(self, caplog):
        invalid_secret_name = "invalid_secret_name"
        region = "eu-west-2"
        with patch("boto3.session.Session.client") as mock_session:
            mock_session.return_value.get_secret_value.side_effect = ClientError(
                {
                    "Error": {
                        "Code": "ResourceNotFoundException",
                        "Message": "The secret does not exist.",
                    }
                },
                "ClientError"
            )
            with caplog.at_level(logging.ERROR):
                get_dw_creds(invalid_secret_name, region)
                assert "Invalid secret name" in caplog.text


@pytest.mark.describe("Connect to db function tests")
class TestDatabaseConnection:

    @pytest.mark.it("Function returns connection")
    def test_func_returns_connection(self):
        assert isinstance(connect_to_db(), Connection)

    @pytest.mark.it("Function raises db error for invalid credentials")
    def test_conn_raises_db_error_for_invalid_creds(self, caplog):
        with patch.dict(DW_CREDS, {"password": "password"}):
            with caplog.at_level(logging.ERROR):
                connect_to_db()
                assert "Database error" in caplog.text

    @pytest.mark.it("Function raises InterfaceError if unable to connect to database")
    def test_conn_raises_interfaceerror_db_busy(self, caplog):
        with patch("load.src.load_lambda.connect") as mock_conn:
            mock_conn.side_effect = InterfaceError
            with caplog.at_level(logging.ERROR):
                connect_to_db()
                assert "Unable to connect to database" in caplog.text

    @pytest.mark.it("Function retries 3 times if unable to connect to database")
    def test_conn_retries_if_unable_to_connect(self):
        with patch("load.src.load_lambda.connect") as mock_conn:
            mock_conn.side_effect = InterfaceError
            connect_to_db()
            assert mock_conn.call_count == 3
