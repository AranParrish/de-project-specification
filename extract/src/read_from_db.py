from extract.src.connection import connect_to_db
import pg8000
from datetime import datetime

def read_history_data_from_address_tb():
    conn = connect_to_db()
    history_data = conn.run("""SELECT * FROM address;""")
    return history_data

def read_updates_from_address_tb():
    conn = connect_to_db()
    updates = conn.run("""SELECT * FROM address WHERE  now() - last_updated < interval '30 minutes';""")
    return updates

def read_deletions_from_address_tb():
    conn = connect_to_db()
    deletions = conn.run("""CREATE OR REPLACE FUNCTION return_deleted_address()
                        RETURNS TRIGGER 
                        AS
                        $$
                        BEGIN
                        RETURN OLD;
                        END;
                        $$
                        LANGUAGE plpgsql;
                        
                        CREATE TRIGGER after_delete_address_trigger
                        AFTER DELETE ON address
                        FOR EACH ROW
                        EXECUTE FUNCTION return_deleted_address();""")
    return deletions


if __name__ == "__main__":
    read_history_data_from_address_tb()
    read_updates_from_address_tb()
    read_deletions_from_address_tb()

# CREATE TABLE ad_archive(
#                         address_id INT NOT NULL,
#                         address_line_1 VARCHAR NOT NULL
#                         address_line_2 VARCHAR
#                         district VARCHAR 
#                         city VARCHAR NOT NULL
#                         postal_code VARCHAR NOT NULL
#                         country VARCHAR NOT NULL
#                         phone VARCHAR NOT NULL
#                         created_at timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
#                         last_updated timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP
#                         deleted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
#                         );
    
                        