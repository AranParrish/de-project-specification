import pytest
from pg8000.native import Connection, InterfaceError
import os
from moto import mock_aws
import boto3
import logging
from unittest.mock import Mock, patch
from botocore.exceptions import ClientError

with patch.dict(os.environ, {"ingestion_zone_bucket": "test_bucket"}):
    from extract.src.extract_lambda import (
        connect_to_db,
        lambda_handler,
        write_data,
        read_history_data_from_any_tb,
        read_updates_from_any_tb,
        get_db_creds,
        DB_CREDS,
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
class TestGetDBCreds:

    @pytest.mark.it("valid name test")
    def test_retrieve_secret_valid_name(self):
        secret_name = "db_creds"
        region = "eu-west-2"
        assert get_db_creds(secret_name, region) != {}

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
                get_db_creds(invalid_secret_name, region)
                assert "Invalid secret name" in caplog.text


@pytest.mark.describe("Connect to db function tests")
class TestDatabaseConnection:

    @pytest.mark.it("Function returns connection")
    def test_func_returns_connection(self):
        assert isinstance(connect_to_db(), Connection)

    @pytest.mark.it("Function raises db error for invalid credentials")
    def test_conn_raises_db_error_for_invalid_creds(self, caplog):
        with patch.dict(DB_CREDS, {"password": "password"}):
            with caplog.at_level(logging.ERROR):
                connect_to_db()
                assert "Database error" in caplog.text

    @pytest.mark.it("Function raises InterfaceError if unable to connect to database")
    def test_conn_raises_interfaceerror_db_busy(self, caplog):
        with patch("extract.src.extract_lambda.Connection") as mock_conn:
            mock_conn.side_effect = InterfaceError
            with caplog.at_level(logging.ERROR):
                connect_to_db()
                assert "Unable to connect to database" in caplog.text

    @pytest.mark.it("Function retries 3 times if unable to connect to database")
    def test_conn_retries_if_unable_to_connect(self):
        with patch("extract.src.extract_lambda.Connection") as mock_conn:
            mock_conn.side_effect = InterfaceError
            connect_to_db()
            assert mock_conn.call_count == 3


@pytest.mark.describe("test read historic data from database")
class TestReadHistoryDataFromDB:
    @pytest.mark.it("if the data is valid return a list")
    def test_func_returns_results_when_valid_table_name_passed(self):
        valid_tb_name = [
            "sales_order",
            "design",
            "currency",
            "staff",
            "counterparty",
            "address",
            "department",
            "purchase_order",
            "payment_type",
            "payment",
            "transaction",
        ]

        for table_name in valid_tb_name:
            results = read_history_data_from_any_tb(table_name)

            assert isinstance(results, list)

    @pytest.mark.it("if table name is invalid return a database error")
    def test_func_returns_message_when_invalid_table_name_passed(self, caplog):
        table_name = "departmen"
        with caplog.at_level(logging.ERROR):
            read_history_data_from_any_tb(table_name)
        assert "not a valid table name" in caplog.text


@pytest.mark.describe("test read updated data from database")
class TestReadUpdateDataFromDB:
    @pytest.mark.it("return updated result in a list")
    def test_func_returns_updated_results_when_valid_table_name_passed(self):
        valid_tb_name = [
            "sales_order",
            "design",
            "currency",
            "staff",
            "counterparty",
            "address",
            "department",
            "purchase_order",
            "payment_type",
            "payment",
            "transaction",
        ]
        for table_name in valid_tb_name:
            results = read_updates_from_any_tb(table_name)
            assert isinstance(results, dict | list)

    @pytest.mark.it("returns an error message if table name is invalid")
    def test_func_returns_message_when_invalid_table_name_passed(self, caplog):
        invalid_tb_name = "products"
        with caplog.at_level(logging.ERROR):
            read_history_data_from_any_tb(invalid_tb_name)
        assert "not a valid table name" in caplog.text


@pytest.mark.describe("Test write data function")
class TestWriteData:
    @pytest.mark.it("Input is not mutated")
    def test_write_data_input_not_mutated(self, s3, bucket):
        test_input = [1, 2, 3]
        copy_test_input = [1, 2, 3]
        test_table = "test"
        write_data(s3, bucket, test_input, test_table) 
        assert test_input == copy_test_input

    @pytest.mark.it("Able to put file in S3 bucket")
    def test_write_to_s3(self, s3, bucket):
        data = [{"a": 1}, {"b": 2}]
        test_table = "test"
        write_data(s3, bucket, data, test_table)
        listing = s3.list_objects_v2(Bucket=bucket)
        assert len(listing["Contents"]) == 1
        assert "test" in listing["Contents"][0]["Key"]

    @pytest.mark.it("Logs client error if there is no S3 bucket")
    def test_write_s3_logs_client_error(self, s3, caplog):
        data = [{"a": 1}, {"b": 2}]
        test_table = "test"
        test_bucket = "test_bucket"
        with caplog.at_level(logging.INFO):
            write_data(s3, test_bucket, data, test_table)
        assert "ClientError" in caplog.text


@pytest.mark.describe("Test lambda handler function")
class TestLambdaHandler:
    @pytest.mark.it("Test for empty ingestion bucket, history read for all tables")
    def test_lambda_handler_empty_bucket(self, s3, bucket):
        lambda_handler(event="event", context="context")
        test_bucket = "test_bucket"
        bucket_content = s3.list_objects_v2(Bucket=test_bucket)
        assert len(bucket_content["Contents"]) == 11

    @pytest.mark.it("Check for updated files")
    def test_lambda_handler_updated_files(self, s3, caplog, bucket):
        lambda_handler(event="event", context="context")
        with caplog.at_level(logging.INFO):
            lambda_handler(event="event", context="context")
            assert "has no new data" in caplog.text

    @pytest.mark.it("Test Client Error")
    def test_client_error(self, caplog):
        with patch("boto3.client") as mock_client:
            mock_client.return_value.list_objects_v2.side_effect = ClientError(
                {
                    "Error": {
                        "Code": "InvalidClientTokenId",
                        "Message": "The security token included in the request is invalid.",
                    }
                },
                "ClientError"
            )
            with caplog.at_level(logging.ERROR):
                lambda_handler(event="event", context="context")
                assert "Error InvalidClientTokenId: " in caplog.text
