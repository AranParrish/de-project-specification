import pytest, os, boto3
import pandas as pd
import awswrangler as wr
from moto import mock_aws
from unittest.mock import patch

# Patch environment variables for both S3 buckets
with patch.dict(os.environ, {"ingestion_zone_bucket": "test_ingestion_bucket", "processed_data_zone_bucket": "test_processed_bucket"}):
    from transform.src.processed_lambda import (
        conversion_for_dim_location,
        conversion_for_dim_currency,
        conversion_for_dim_design,
        conversion_for_dim_counterparty,
        conversion_for_dim_staff,
        date_helper,
        conversion_for_dim_date,
        conversion_for_fact_sales_order,
        lambda_handler
    )

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
    with open("transform/tests/test_data/sales_order-15_36_42.731009.json") as f:
        text_to_write = f.read()
        s3.put_object(
            Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/sales_order-15_36_42.731009.json"
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

    @mock_aws(config={"s3": {"use_docker": False}})
    @pytest.mark.it('Initilisation test')
    def test_transform_lambda_initilisation(self, test_ingestion_bucket, test_processed_bucket, s3):
        lambda_handler({}, None)
        assert s3.list_objects_v2(Bucket='test_ingestion_bucket')['KeyCount'] == s3.list_objects_v2(Bucket='test_processed_bucket')['KeyCount']
        
    
    @mock_aws(config={"s3": {"use_docker": False}})
    @pytest.mark.it('Check Object Key Content')
    def test_transform_lambda_content(self, test_ingestion_bucket, test_processed_bucket, s3):
        lambda_handler({}, None)
        response = s3.get_object(Bucket='test_processed_bucket', Key= "2024-05-21/sales_order-15_36_42.731009.parquet" )
        
        df = wr.s3.read_parquet(path=f"s3://test_processed_bucket/2024-05-21/sales_order-15_36_42.731009.parquet")
        
        expected = ['sales_record_id', 'sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id',
       'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date',
       'agreed_payment_date', 'agreed_delivery_location_id', 'created_date',
       'created_time', 'last_updated_date', 'last_updated_time']
        
        assert all([ col in expected for col in df.columns])
            
        
    