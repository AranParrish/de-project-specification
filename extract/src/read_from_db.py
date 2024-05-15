from extract.src.connection import connect_to_db
import pg8000
from datetime import datetime

# function that reads entire contents of table at current time

def read_history_data_from_any_tb(tb_name):
    valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        history_data = conn.run(f"""SELECT * FROM {tb_name};""")
        print(history_data)
        return (history_data, conn)
    else:
        return f"{tb_name} is not a valid table name."

def read_updates_from_address_tb(tb_name):
    valid_tb_name = ['sales_order', 'design', 'currency', 'staff', 'counterparty', 'address', 'department', 'purchase_order', 'payment_type', 'payment', 'transaction' ]
    if tb_name in valid_tb_name:
        conn = connect_to_db()
        updates = conn.run(f"""SELECT * FROM {tb_name} WHERE  now() - last_updated < interval '20 minutes';""")
        return (updates, conn)
    else:
        return f"{tb_name} is not a valid table name."

# if __name__ == "__main__":
#     read_history_data_from_address_tb()
#     print(read_updates_from_address_tb('staff'))
#     print(read_history_data_from_any_tb('currenc'))

                        