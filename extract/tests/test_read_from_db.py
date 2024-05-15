from extract.src.read_from_db import read_history_data_from_any_tb, read_updates_from_any_tb
from pg8000.native import Connection

class TestReadHistoryDataFromDB:

    def test_func_returns_results_when_valid_table_name_passed(self):
        valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 
                         'address', 'department', 'purchase_order', 'payment_type', 
                         'payment', 'transaction' ]
        
        for table_name in valid_tb_name:
            results = read_history_data_from_any_tb(table_name)
            
            assert isinstance(results[0], list)
            assert isinstance(results[1], Connection)
    
    def test_func_returns_message_when_invalid_table_name_passed(self):
        table_name = "departmen"
        results = read_history_data_from_any_tb(table_name)
        
        assert results == f"{table_name} is not a valid table name."
        
class TestReadUpdateDataFromDB:

    def test_func_returns_updated_results_when_valid_table_name_passed(self):
        valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 
                         'address', 'department', 'purchase_order', 'payment_type', 
                         'payment', 'transaction' ]
        for table_name in valid_tb_name:
            results = read_updates_from_any_tb(table_name)
        
            assert isinstance(results[0], list)
            assert isinstance(results[1], Connection)
    
    def test_func_returns_message_when_invalid_table_name_passed(self):
        table_name = "products"
        results = read_updates_from_any_tb(table_name)
        
        assert results == f"{table_name} is not a valid table name."
        
    