import pytest, os, boto3, datetime, json
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
        conversion_for_fact_sales_order,
        process_file,
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


@pytest.mark.describe("test conversion_for_dim_location")
class TestDimLocation:
    input_file = 'transform/tests/data/address.json'
    location_df = pd.read_json(input_file)
    output_df = conversion_for_dim_location(location_df)

    @pytest.mark.it("check the number of columns")
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

    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 3

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

    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 4

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

    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 9

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

    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 6

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
        
@pytest.mark.describe("test conversion_for_dim_date_tb")
class TestDimDateTb:
    
    df = date_helper()
    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        assert len(self.df.columns) == 8
        
    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        assert isinstance(self.df, pd.DataFrame)
        
    @pytest.mark.it("check column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['date_id','year','month','day','day_of_week','day_name','month_name','quarter']
        for column in self.df.columns:
            assert column in expected_columns


@pytest.mark.describe("test conversion_for_fact_sales_order")
class TestFactSalesOrder:
    input_file = 'transform/tests/data/sales_order.json'
    sales_df = pd.read_json(input_file)
    output_df = conversion_for_fact_sales_order(sales_df)

    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 14

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ["sales_record_id","sales_order_id","created_date","created_time","last_updated_date","last_updated_time","sales_staff_id","counterparty_id","units_sold","unit_price","currency_id","design_id","agreed_payment_date","agreed_delivery_date","agreed_delivery_location_id"]
        for column in self.output_df.columns:
            assert column in expected_columns


    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        
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

@pytest.mark.describe('test process_file')
class TestProcessFile:

    @pytest.mark.it('check correct function called for sales_order key')
    def test_correct_function_call_for_sales_order(self, s3, test_ingestion_bucket, test_processed_bucket):
        with patch("transform.src.processed_lambda.conversion_for_fact_sales_order") as mock_func:
            process_file(s3, "2024-05-21/sales_order-15_36_42.731009.json")
        assert mock_func.called

    @pytest.mark.it('check correct function called for address key')
    def test_correct_function_call_for_address(self, s3, test_ingestion_bucket, test_processed_bucket):
        with open("transform/tests/data/address.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/address-15_36_42.731009.json"
            )
     
        with patch("transform.src.processed_lambda.conversion_for_dim_location") as mock_func:
            process_file(s3, "2024-05-21/address-15_36_42.731009.json")
        assert mock_func.called

    @pytest.mark.it('check correct function called for counterparty key')
    def test_correct_function_call_for_counterparty(self, s3, test_ingestion_bucket, test_processed_bucket):
        # input_ad_file = "transform/tests/data/address.json"
        # df = pd.read_json(input_ad_file)
        with open("transform/tests/data/address.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/address-15_36_42.731009.json"
            )
            lambda_handler({}, None)
        with open("transform/tests/data/counterparty.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/counterparty-15_36_42.731009.json"
            )
        with patch("transform.src.processed_lambda.conversion_for_dim_counterparty") as mock_func:
            process_file(s3, "2024-05-21/counterparty-15_36_42.731009.json")
        assert mock_func.called

    @pytest.mark.it('check correct function called for staff key')
    def test_correct_function_call_for_department(self, s3, test_ingestion_bucket, test_processed_bucket):
        with open("transform/tests/data/department.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/department-15_36_42.731009.json"
            )
            lambda_handler({}, None)
        with open("transform/tests/data/staff.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/staff-15_36_42.731009.json"
            )
        with patch("transform.src.processed_lambda.conversion_for_dim_staff") as mock_func:
            process_file(s3, "2024-05-21/staff-15_36_42.731009.json")
        assert mock_func.called

    @pytest.mark.it('check correct function called for design key')
    def test_correct_function_call_for_design(self, s3, test_ingestion_bucket, test_processed_bucket):
        with open("transform/tests/data/design.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/design-15_36_42.731009.json"
            )
        with patch("transform.src.processed_lambda.conversion_for_dim_design") as mock_func:
            process_file(s3, "2024-05-21/design-15_36_42.731009.json")
        assert mock_func.called        

    @pytest.mark.it('check correct function called for currency key')
    def test_correct_function_call_for_currency(self, s3, test_ingestion_bucket, test_processed_bucket):
        with open("transform/tests/data/currency.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/currency-15_36_42.731009.json"
            )
        with patch("transform.src.processed_lambda.conversion_for_dim_currency") as mock_func:
            process_file(s3, "2024-05-21/currency-15_36_42.731009.json")
        assert mock_func.called


# Add tests for writing to the processed data bucket

@pytest.mark.describe('Transform lambda handler tests')
class TestTransfomLambdaHandler:

    @mock_aws(config={"s3": {"use_docker": False}})
    @pytest.mark.it('Initialisation test')
    def test_transform_lambda_initialisation(self, s3, test_ingestion_bucket, test_processed_bucket):
        lambda_handler({}, None)
        assert s3.list_objects_v2(Bucket="test_ingestion_bucket")['KeyCount']  == s3.list_objects_v2(Bucket="test_processed_bucket")['KeyCount']

    @mock_aws(config={"s3": {"use_docker": False}})
    @pytest.mark.it('Check Object Key Content')
    def test_transform_lambda_content(self, test_ingestion_bucket, test_processed_bucket, s3):
        lambda_handler({}, None)
        df = wr.s3.read_parquet(path=f"s3://test_processed_bucket/2024-05-21/fact_sales_order-15_36_42.731009.parquet")

        expected = [ 'sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id',
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
        with patch.dict(os.environ,{"ingestion_zone_bucket": "fake_ingestion_bucket"}):
            with caplog.at_level(logging.ERROR):
                    lambda_handler(event="event", context="context")
                    assert "No such bucket" in caplog.text
                        
    
    @pytest.mark.it('Test S3 NoSuchKey response')
    def test_s3_no_such_key_response(self, caplog, s3, test_ingestion_bucket, test_processed_bucket):
        with patch("transform.src.processed_lambda.process_file") as mock_process_file_func:
            mock_process_file_func.side_effect = ClientError(
                {
                    "Error":{
                        "Code": "NoSuchKey",
                        "Message": "This key does not exist."
                    }
                }, 
                "ClientError - NoSuchKey"
            )
            with caplog.at_level(logging.ERROR):
                lambda_handler(event="event", context="context")
            assert "This key does not exist" in caplog.text
    
    @pytest.mark.it('Test decoding error response')
    def test_decoding_error_response(self, caplog, s3, test_ingestion_bucket, test_processed_bucket):
        with patch("transform.src.processed_lambda.process_file") as mock_process_file_func:
            mock_process_file_func.side_effect = UnicodeDecodeError("utf-8", b"\xff", 0, 1, "Unrecognised character")
            with caplog.at_level(logging.ERROR):
                lambda_handler(event="event", context="context")
            assert "Unable to decode the file" in caplog.text

    @pytest.mark.it('Test KeyError response')
    def test_keyerror_repsonse(self, caplog, s3, test_ingestion_bucket, test_processed_bucket):
        lambda_handler({}, "context")
        with caplog.at_level(logging.ERROR):
            lambda_handler(event={}, context='context')
        assert "Error retrieving data" in caplog.text

   
    @pytest.mark.it('Test RuntimeError raised for other errors')
    def test_runtime_error_raised(self, caplog, s3, test_ingestion_bucket, test_processed_bucket):
        with patch("transform.src.processed_lambda.process_file") as mock_process_file_func:
            mock_process_file_func.side_effect = NameError
            with pytest.raises(RuntimeError):
                lambda_handler(event="event", context="context")


@pytest.mark.describe("Test lambda handler event trigger")
class TestLambdaEventTrigger:

    @pytest.mark.it("if there is a valid event and correct type put the object into processed s3 bucket")
    def test_valid_event(self, s3, valid_event, test_processed_bucket, test_ingestion_bucket):
        with open("transform/tests/data/sales_order.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_processed_bucket", Key="2024-05-21/sales_order-15_36_42.731009.json"
            )
        lambda_handler(valid_event, {})
        response = s3.list_objects_v2(Bucket="test_processed_bucket")
        assert response["KeyCount"] == 2
        assert response['Contents'][0]['Key'] == '2024-05-21/fact_sales_order-15_36_42.731009.parquet'

    @pytest.mark.it("lambda throws logs message if is not valid json type")
    def test_invalid_type(self, file_type_event, caplog, s3, test_processed_bucket, test_ingestion_bucket):
        with open("transform/tests/data/sales_order.json") as f:
            text_to_write = f.read()
            s3.put_object(
                Body=text_to_write, Bucket="test_processed_bucket", Key="2024-05-21/sales_order-15_36_42.731009.json"
            )
        with caplog.at_level(logging.ERROR):
            lambda_handler(file_type_event, {})
            assert "File is not a valid json file" in caplog.text

    @pytest.mark.it("if there is an invalid event returns exception error")
    def test_invalid_event(self, s3, invalid_event, caplog):
        with caplog.at_level(logging.ERROR):
            lambda_handler(invalid_event, {})
        assert "No such bucket" in caplog.text
        
# @pytest.mark.describe("Test Dim Date")
# class TestCheckDimDate:

#     @pytest.mark.it("if there is no  dim date in processed zone bucket then seed the dim date")
#     def test_ingest_dim_date(self, s3, valid_event, test_processed_bucket, test_ingestion_bucket):
#         with open("transform/tests/data/sales_order.json") as f:
#             text_to_write = f.read()
#             s3.put_object(
#                 Body=text_to_write, Bucket="test_ingestion_bucket", Key="2024-05-21/payment_order-15_36_42.731009.json"
#             )
#         lambda_handler(valid_event, {})
#         response = s3.list_objects_v2(Bucket="test_processed_bucket")
#         assert response["KeyCount"] == 2
#         assert response['Contents'][0]['Key'] == '2024-05-21/dim_date-15_36_42.731009.parquet'
        
#     @pytest.mark.it("if there is dim date in processed zone bucket then do not seed the dim date")
#     def test_does_not_ingest_dim_date_when_already_present(self, s3, valid_event, test_processed_bucket, test_ingestion_bucket):
#         with open("transform/tests/data/sales_order.json") as f:
#             text_to_write = f.read()
#             s3.put_object(
#                 Body=text_to_write, Bucket="test_processed_bucket", Key="2024-05-21/dim_date-15_36_42.731009.parquet"
#             )
#         lambda_handler(valid_event, {})
#         response = s3.list_objects_v2(Bucket="test_processed_bucket")
#         assert response["KeyCount"] == 2
#         assert response['Contents'][0]['Key'] == '2024-05-21/dim_date-15_36_42.731009.parquet'