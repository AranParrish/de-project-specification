import pandas as pd
import pytest
import datetime

from load.src.convert_to_parquet import conversion_for_dim_location,\
    conversion_for_dim_currency, conversion_for_dim_design, \
    conversion_for_dim_counterparty, conversion_for_dim_staff, \
    conversion_for_dim_date_helper

@pytest.mark.describe("test conversion_for_dim_location")
class TestDimLocation:
    input_file = 'load/tests/data/address.json'
    output_df = conversion_for_dim_location(input_file)[1]
    output_df_table_name = conversion_for_dim_location(input_file)[0]

    @pytest.mark.it("check the number of columns without primary key column")
    def test_number_of_columns(self):
        assert len(self.output_df.columns) == 7

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['address_line_1','address_line_2','district','city','postal_code','country','phone']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check index column is location_id")
    def test_index_column_is_location_id(self):
        assert self.output_df.index.name == 'location_id'

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
       for column in self.output_df.columns:
           assert type(column) ==  str

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        assert self.output_df_table_name == 'dim_location'
        assert isinstance(self.output_df, pd.DataFrame)

@pytest.mark.describe("test conversion_for_dim_currency")
class TestDimCurrency:
    input_file = 'load/tests/data/currency.json'
    output_df = conversion_for_dim_currency(input_file)[1]
    output_df_table_name = conversion_for_dim_currency(input_file)[0]

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['currency_code','currency_name']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check index column is currency_id")
    def test_index_column_is_location_id(self):
        assert self.output_df.index.name == 'currency_id'

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        assert self.output_df_table_name == 'dim_currency'
        assert isinstance(self.output_df, pd.DataFrame)
    
@pytest.mark.describe("test conversion_for_dim_design")
class TestDimDesign:
    input_file = 'load/tests/data/design.json'
    output_df = conversion_for_dim_design(input_file)[1]
    output_df_table_name = conversion_for_dim_design(input_file)[0]

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['design_name','file_location','file_name']
        assert list(self.output_df.columns) == expected_columns

    @pytest.mark.it("check index column is design_id")
    def test_index_column_is_location_id(self):
        assert self.output_df.index.name == 'design_id'

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        assert self.output_df_table_name == 'dim_design'
        assert isinstance(self.output_df, pd.DataFrame)

@pytest.mark.describe("test conversion_for_dim_counterparty")
class TestDimCounterparty:
    
    input_ad_file = 'load/tests/data/address.json'
    input_cp_file = 'load/tests/data/counterparty.json'
    output_df = conversion_for_dim_counterparty(input_ad_file,input_cp_file)[1]
    output_df_table_name = conversion_for_dim_counterparty(input_ad_file, input_cp_file)[0]

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['counterparty_legal_name', 'counterparty_legal_address_line_1','counterparty_legal_address_line_2','counterparty_legal_district','counterparty_legal_city','counterparty_legal_postal_code','counterparty_legal_country','counterparty_legal_phone_number']
        assert list(self.output_df.columns) == expected_columns


    @pytest.mark.it("check index column is counterparty_id")
    def test_index_column_is_location_id(self):
        assert self.output_df.index.name == 'counterparty_id'

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        assert self.output_df_table_name == 'dim_counterparty'
        assert isinstance(self.output_df, pd.DataFrame)

@pytest.mark.describe("test conversion_for_dim_staff")
class TestDimStaff:
    input_dep_file = 'load/tests/data/department.json'
    input_staff_file = 'load/tests/data/staff.json'
    output_df = conversion_for_dim_staff(input_dep_file,input_staff_file)[1]
    output_df_table_name = conversion_for_dim_staff(input_dep_file, input_staff_file)[0]

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        expected_columns = ['first_name','last_name','department_name','location','email_address']
        for column in self.output_df.columns:
            assert column in expected_columns


    @pytest.mark.it("check index column is staff_id")
    def test_index_column_is_location_id(self):
        assert self.output_df.index.name == 'staff_id'

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        for column in self.output_df.columns:
            assert type(column) == str

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        assert self.output_df_table_name == 'dim_staff'
        assert isinstance(self.output_df, pd.DataFrame)

@pytest.mark.describe("test conversion_for_dim_date_helper")
class TestDimDateHelper:
    input_file = 'load/tests/data/sales_order.json'
    df = pd.read_json(input_file)
    column = 'created_at'
    created_at_df = df[[column]]
    output_df = conversion_for_dim_date_helper(created_at_df, column)
    

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

    @pytest.mark.it("check there are no duplicate rows")
    def test_no_duplicate_rows(self):
        pass

    @pytest.mark.it("check index column is date_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass

@pytest.mark.describe("test conversion_for_fact_sales_order")
class TestFactSalesOrder:

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check index column is sales_record_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass


