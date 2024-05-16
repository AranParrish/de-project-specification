from extract.src.extract_lambda_formatter import format_data
import pytest
from extract.src.connection import connect_to_db


@pytest.fixture(autouse=True)
def run_db():
    db = connect_to_db()
    yield db
    db.close()


@pytest.mark.describe('Format data function unit tests')
class TestFormatData:

    @pytest.mark.it('Returns expected format and keys for a single row query result on sales order table')
    def test_format_data_single_item(self, run_db):
        test_query_result = run_db.run("""SELECT * FROM sales_order WHERE sales_order_id = 1;""")
        output = format_data(test_query_result, run_db)
        expected_keys = ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id',
                         'counterparty_id', 'units_sold', 'unit_price', 'currency_id',
                         'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
        assert isinstance(output, dict)
        assert all([key in output.keys() for key in expected_keys])

    @pytest.mark.it('Returns expected format and keys for a multiple row query result on design table')
    def test_format_data_multiple_items(self, run_db):
        test_query_result = run_db.run("""SELECT * FROM design LIMIT 5;""")
        output = format_data(test_query_result, run_db)
        expected_keys = ['design_id', 'created_at', 'last_updated', 'design_name', 'file_location',
                         'file_name']
        assert isinstance(output, list)
        for result in output:
            assert all([key in result.keys() for key in expected_keys])