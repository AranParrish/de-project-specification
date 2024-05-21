import pandas as pd
import pytest

from load.src.convert_to_parquet import conversion_for_dim_location,\
    conversion_for_dim_currency, conversion_for_dim_design, \
    conversion_for_dim_counterparty, conversion_for_dim_staff, \
    conversion_for_dim_date_helper

@pytest.mark.describe("test conversion_for_dim_location")
class TestDimLocation:

    @pytest.mark.it("check the number of columns")
    def test_valid_file_input():
        # added address.json at tests/data for testing
        pass

    @pytest.mark.it("check the column name")
    def test_valid_colomn_name():
        pass



