import pytest, os, boto3, datetime
import pandas as pd
import awswrangler as wr
from moto import mock_aws
from unittest.mock import patch
from botocore.exceptions import ClientError
import logging



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

@pytest.mark.describe("test conversion_for_dim_location")
class TestDimLocation:
    input_file = 'transform/tests/data/address.json'
    location_df = pd.read_json(input_file)
    output_df = conversion_for_dim_location(location_df)

    @pytest.mark.it("check the number of columns without primary key column")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 8

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['location_id', 'address_line_1','address_line_2','district','city','postal_code','country','phone']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
       for column in self.output_df.columns:
           assert type(column) ==  str

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test conversion_for_dim_currency")
class TestDimCurrency:
    input_file = 'transform/tests/data/currency.json'
    currency_df = pd.read_json(input_file)
    output_df = conversion_for_dim_currency(currency_df)

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['currency_id', 'currency_code','currency_name']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test conversion_for_dim_design")
class TestDimDesign:
    input_file = 'transform/tests/data/design.json'
    design_df = pd.read_json(input_file)
    output_df = conversion_for_dim_design(design_df)

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['design_id', 'design_name','file_location','file_name']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test conversion_for_dim_counterparty")
class TestDimCounterparty:
    
    input_ad_file = 'transform/tests/data/address.json'
    input_cp_file = 'transform/tests/data/counterparty.json'
    address_df = pd.read_json(input_ad_file)
    counterparty_df = pd.read_json(input_cp_file)
    output_df = conversion_for_dim_counterparty(address_df,counterparty_df)

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1','counterparty_legal_address_line_2','counterparty_legal_district','counterparty_legal_city','counterparty_legal_postal_code','counterparty_legal_country','counterparty_legal_phone_number']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test conversion_for_dim_staff")
class TestDimStaff:
    input_dep_file = 'transform/tests/data/department.json'
    input_staff_file = 'transform/tests/data/staff.json'
    department_df = pd.read_json(input_dep_file)
    staff_df = pd.read_json(input_staff_file)
    output_df = conversion_for_dim_staff(department_df,staff_df)

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['staff_id', 'first_name','last_name','department_name','location','email_address']
        for column in self.output_df.columns:
            assert column in expected_columns

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test date_helper")
class TestDimDateHelper:
    input_file = 'transform/tests/data/sales_order.json'
    df = pd.read_json(input_file)
    column = 'created_at'
    created_at_df = df[[column]]
    output_df = date_helper(created_at_df, column)

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['date_id','year','month','day','day_of_week','day_name','month_name','quarter']
        for column in self.output_df.columns:
            assert column in expected_columns

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        assert self.output_df.date_id.dtype == object
        assert self.output_df.year.dtype == 'Int32'
        assert self.output_df.month.dtype == 'Int32'
        assert self.output_df.day.dtype == 'Int32'
        assert self.output_df.day_of_week.dtype == 'Int32'
        assert self.output_df.day_name.dtype == 'string[python]'
        assert self.output_df.month_name.dtype == 'string[python]'
        assert self.output_df.quarter.dtype == 'Int32'

    @pytest.mark.it("check values in date_id are of date type")
    def test_values_in_date_id(self):
        for i in self.output_df.index:
            assert isinstance(self.output_df.loc[i,"date_id"], datetime.date)

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test conversion_for_dim_date_tb")
class TestDimDateTb:
    input_file = 'transform/tests/data/sales_order.json'
    date_df = pd.read_json(input_file)
    output_df = conversion_for_dim_date(date_df)

    @pytest.mark.it("check there are no duplicate rows")
    def test_no_duplicate_rows(self):
        result = self.output_df.duplicated()
        assert all([value == False for value in result.values])

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)


@pytest.mark.describe("test conversion_for_fact_sales_order")
class TestFactSalesOrder:
    input_file = 'transform/tests/data/sales_order.json'
    sales_df = pd.read_json(input_file)
    output_df = conversion_for_fact_sales_order(sales_df)

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ["sales_record_id", "sales_order_id","created_date","created_time","last_updated_date","last_updated_time","sales_staff_id","counterparty_id","units_sold","unit_price","currency_id","design_id","agreed_payment_date","agreed_delivery_date","agreed_delivery_location_id"]
        for column in self.output_df.columns:
            assert column in expected_columns


    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        assert self.output_df.sales_record_id.dtype == 'int64'
        assert self.output_df.sales_order_id.dtype == 'int64'
        assert self.output_df.design_id.dtype == 'int64'
        assert self.output_df.sales_staff_id.dtype == 'int64'
        assert self.output_df.counterparty_id.dtype == 'int64'
        assert self.output_df.units_sold.dtype == 'int64'
        assert self.output_df.unit_price .dtype == 'float64'
        assert self.output_df.currency_id.dtype == 'int64'
        assert self.output_df.agreed_delivery_date.dtype == object
        assert self.output_df.agreed_payment_date.dtype == object
        assert self.output_df.agreed_delivery_location_id.dtype == 'int64'
        assert self.output_df.created_date.dtype == object
        assert self.output_df.created_time.dtype == object
        assert self.output_df.last_updated_date.dtype == object
        assert self.output_df.last_updated_time.dtype == object

    @pytest.mark.it("check values of agreed_delivery_date, agreed_payment_date, created_date, last_updated_date are of date type")
    def test_values_in_date_type(self):
        for i in self.output_df.index:
            assert isinstance(self.output_df.loc[i,"agreed_delivery_date"], datetime.date)
            assert isinstance(self.output_df.loc[i,"agreed_payment_date"], datetime.date)
            assert isinstance(self.output_df.loc[i,"created_date"], datetime.date)
            assert isinstance(self.output_df.loc[i,"last_updated_date"], datetime.date)
    
    @pytest.mark.it("check values of created_time and last_updated_time are of time type")
    def test_values_in_time_type(self):
        for i in self.output_df.index:
            assert isinstance(self.output_df.loc[i,"created_time"], datetime.time)
            assert isinstance(self.output_df.loc[i,"last_updated_time"], datetime.time)

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.output_df, pd.DataFrame)

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
    with open("transform/tests/data/sales_order.json") as f:
        text_to_write = f.read()
        s3.put_object(
            Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/sales_order-15_36_42.731009.json"
        )


@pytest.fixture
def test_processed_bucket(s3):
    s3.create_bucket(
        Bucket='test_processed_bucket',
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )

# Add tests from util functions

# Add tests for writing to the processed data bucket

# checks the number of files in test_ingestion_bucket are the same in the test_processed_bucket
@pytest.mark.describe('Transform lambda handler tests')
class TestTransfomLambdaHandler:

    # @mock_aws(config={"s3": {"use_docker": False}})
    @pytest.mark.it('Initialisation test')
    def test_transform_lambda_initialisation(self, test_ingestion_bucket, test_processed_bucket, s3):
        lambda_handler({}, None)
        assert s3.list_objects_v2(Bucket="test_ingestion_bucket")['KeyCount'] == s3.list_objects_v2(Bucket="test_processed_bucket")['KeyCount']
        
# checks the parquet files has the required columns as specified in the schema

    # @mock_aws(config={"s3": {"use_docker": False}})
    @pytest.mark.it('Check Object Key Content')
    def test_transform_lambda_content(self, test_ingestion_bucket, test_processed_bucket, s3):
        lambda_handler({}, None)

        df = wr.s3.read_parquet(path=f"s3://test_processed_bucket/2024-05-21/fact_sales_order-15_36_42.731009.parquet")

        expected = ['sales_record_id', 'sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id',
       'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date',
       'agreed_payment_date', 'agreed_delivery_location_id', 'created_date',
       'created_time', 'last_updated_date', 'last_updated_time']
        assert all([ col in expected for col in df.columns])

    @pytest.mark.it('Test CLientError response')
    def test_client_error_response(self, caplog, s3, test_ingestion_bucket, test_processed_bucket):
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
                
    @pytest.mark.it('Test S3 NoSuchBucket response')
    def test_s3_no_such_bucket_response(self, caplog, s3):
        # with patch.dict(os.environ,{"ingestion_zone_bucket": "fake_ingestion_bucket"}):
        with caplog.at_level(logging.ERROR):
                lambda_handler(event="event", context="context")
                assert "No such bucket" in caplog.text
                    
    @pytest.mark.it('Test S3 NoSuchKey response')

    def test_s3_no_such_key_response(self, caplog, s3, test_ingestion_bucket, test_processed_bucket):

        with patch("transform.src.transform_lambda.boto3.client") as mock_client:
            mock_client.return_value.get_object.side_effect = ClientError(
                {
                    "Error": {
                        "Code": 'NoSuchKey',
                        "Message": "No such key exists."
                    }
                }, 
                "ClientError"
            )
            with caplog.at_level(logging.ERROR):
                lambda_handler(event="event", context="context")
                print("this is the log message", caplog.text)
                assert "No such key " in caplog.text
            
        #     ClientError['Error']['Code'] = "NoSuchKey"
        #     mock_client.return_value.get_object.side_effect = ClientError.response['Error']['Code']
            
                
        # #         {
        # #           "Error":{
        # #             'Code': "NoSuchKey",
        # #             'Message': "No such key"
        # #         }
        # #     },
        # #       "ClientError"
        # # )

            
        #     with caplog.at_level(logging.ERROR):
        #         lambda_handler(event="event", context="context")
        #         assert "No such key" in caplog.text
                    
    #    lambda_handler('event', None)
    #    response = s3.get_object(Bucket='test_processed_bucket', Key="fake_key")
       
    #    assert "No such key" in response['Error']['Message']