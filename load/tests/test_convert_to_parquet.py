import pandas as pd
import pytest

from load.src.convert_to_parquet import conversion_for_dim_location,\
    conversion_for_dim_currency, conversion_for_dim_design, \
    conversion_for_dim_counterparty, conversion_for_dim_staff, \
    conversion_for_dim_date_helper

@pytest.mark.describe("test conversion_for_dim_location")
class TestDimLocation:

    @pytest.mark.it("check the number of columns")
    def test_number_of_columns(self):
        # added address.json at tests/data for testing
        pass

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check index column is location_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass

@pytest.mark.describe("test conversion_for_dim_currency")
class TestDimCurrency:

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check index column is currency_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass
    
@pytest.mark.describe("test conversion_for_dim_design")
class TestDimDesign:

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check index column is design_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass

@pytest.mark.describe("test conversion_for_dim_counterparty")
class TestDimCounterparty:

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check index column is counterparty_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass

@pytest.mark.describe("test conversion_for_dim_staff")
class TestDimStaff:

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check index column is staff_id")
    def test_index_column_is_location_id(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is correct table name and dataframe")
    def test_output_is_correct_table_name_and_dataframe(self):
        pass

@pytest.mark.describe("test conversion_for_dim_date_helper")
class TestDimDateHelper:

    @pytest.mark.it("check the column names match schema")
    def test_valid_column_names_only(self):
        pass

    @pytest.mark.it("check the column datatypes match schema")
    def test_column_data_types_match_schema(self):
        pass

    @pytest.mark.it("check output is a dataframe")
    def test_output_is_a_dataframe(self):
        pass

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


