from extract.src.extract_lambda_formatter import format_data
import pytest
# Suggest adding a separate util function for the database connection (and import for tests)

# Example unit tests for data formatter taken from a previous sprint.

# Amend fixture to connect to RDS test database
@pytest.fixture(autouse=True)
def run_db():
    db = connect_to_db()
    seed_db()
    yield db
    db.close()

@pytest.mark.describe('Format data function unit tests')
class TestUnitTests:

    @pytest.mark.it('Returns expected result for a single row query result')
    def test_format_data_single_item(self, run_db):
        # Amend to use RDS test database
        result = run_db.run("""SELECT * FROM treasures WHERE treasure_id = 1;""")
        assert format_data(result, run_db) == {'treasure_id': 1, 'treasure_name': 'treasure-a', 'colour': 'turquoise', 
                                                'age': 200, 'cost_at_auction': 20.0, 'shop_id': 1}

    @pytest.mark.it('Returns expected result for a multiple row query result')    
    def test_format_data_multiple_items(self, run_db):
        # Amend to use RDS test database
        result = run_db.run("""SELECT * FROM treasures WHERE treasure_id < 3;""")
        assert format_data(result, run_db) == [{'treasure_id': 1, 'treasure_name': 'treasure-a', 'colour': 'turquoise', 'age': 200, 'cost_at_auction': 20.0, 'shop_id': 1}, {'treasure_id': 2, 'treasure_name': 'treasure-d', 'colour': 'azure', 'age': 100, 'cost_at_auction': 1001.0, 'shop_id': 2}]
