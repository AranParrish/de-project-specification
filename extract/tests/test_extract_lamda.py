from extract.src.extract_lambda import connect_to_db, read_history_data_from_any_tb, read_updates_from_any_tb, lambda_handler
import pytest
from pg8000.native import Connection, DatabaseError
import os
from moto import mock_aws
import boto3
import logging
from unittest.mock import Mock, patch

with patch.dict(os.environ, {"ingestion_zone_bucket": "test_bucket"}):
    from extract.src.extract_lambda import write_data

@pytest.fixture(autouse=True)
def run_db():
    db = connect_to_db()
    yield db
    db.close()

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


@pytest.mark.describe("test connection to database")
class TestDatabaseConnection:
    def test_func_returns_connection(self):
        assert isinstance(connect_to_db(), Connection)


@pytest.mark.describe("test read historic data from database")
class TestReadHistoryDataFromDB:
    @pytest.mark.it('if the data is valid return a list')
    def test_func_returns_results_when_valid_table_name_passed(self):
        valid_tb_name = ['sales_order', 'design', 'currency', 'staff',
                         'counterparty',
                         'address', 'department', 'purchase_order',
                         'payment_type',
                         'payment', 'transaction']

        for table_name in valid_tb_name:
            results = read_history_data_from_any_tb(table_name)

            assert isinstance(results, list)

# write a python to manage db errors!!!!
    @pytest.mark.skip()
    @pytest.mark.it('if table name is invalid return a database error')
    def test_func_returns_message_when_invalid_table_name_passed(self):
        table_name = "departmen"
        with pytest.raises(DatabaseError) as error:
            read_history_data_from_any_tb(table_name)
            message = f'{table_name} is not a valid table name.'
        assert str(error.value) == message


@pytest.mark.describe("test read updated data from database")
class TestReadUpdateDataFromDB:
    @pytest.mark.it('return updated result in a list')
    def test_func_returns_updated_results_when_valid_table_name_passed(self):
        valid_tb_name = ['sales_order', 'design', 'currency', 'staff',
                         'counterparty', 'address', 'department',
                         'purchase_order', 'payment_type',
                         'payment', 'transaction']
        for table_name in valid_tb_name:
            results = read_updates_from_any_tb(table_name)
            assert isinstance(results, dict | list)

    # write a python to manage db errors!!!!
    @pytest.mark.skip()    
    @pytest.mark.it('returns an error message if table name is invalid')
    def test_func_returns_message_when_invalid_table_name_passed(self):
        invalid_tb_name = 'products'
        with pytest.raises(DatabaseError) as error:
            read_history_data_from_any_tb(invalid_tb_name)
            message = f'{invalid_tb_name} is not a valid table name.'
        assert str(error.value) == message


@pytest.mark.describe('Write data input tests')
class TestWriteDataInputs:

    @pytest.mark.it('Input is not mutated')
    def test_write_data_input_not_mutated(self, s3):
        test_input = [1, 2, 3]
        copy_test_input = [1, 2, 3]
        test_table = 'test'
        test_bucket = 'test_bucket'
        write_data(s3, test_bucket, test_input, test_table)
        assert test_input == copy_test_input

@pytest.mark.describe('Write data S3 bucket tests')
class TestWriteDataToS3:

    @pytest.mark.it("Able to put file in S3 bucket")
    def test_write_to_s3(self, s3, data):
        data = [{"a": 1}, {"b": 2}]
        test_table = 'test'
        test_bucket = 'test_bucket'
        resp = write_data(s3, test_bucket, data, test_table)
        listing = s3.list_objects_v2(Bucket="test_bucket")
        assert len(listing["Contents"]) == 1
        assert '_totesys_snapshot' in listing["Contents"][0]["Key"]
        assert resp

    @pytest.mark.it("Logs client error if there is no S3 bucket")
    def test_write_s3_logs_client_error(self, s3, caplog):
        data = [{"a": 1}, {"b": 2}]
        test_table = 'test'
        test_bucket = 'test_bucket2'
        with caplog.at_level(logging.INFO):
            resp = write_data(s3, test_bucket, data, test_table)
        assert not resp
        assert "ClientError" in caplog.text