-- Connect to the source database and generate the schema
\c totesys

-- Generate the schema creation commands and save them to a file
COPY (
    SELECT 
        'CREATE TABLE ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || ' AS TABLE ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || ' WITH NO DATA;'
    FROM pg_tables 
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
) TO '/tmp/schema.sql';

-- Generate data copy commands and save them to a file
COPY (
    SELECT 
        'COPY ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || ' TO ' || quote_literal('/tmp/' || schemaname || '.' || tablename || '.csv') || ' WITH CSV HEADER;'
    FROM pg_tables 
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
) TO '/tmp/copy_commands.sql';

-- Execute the data copy commands to export data to CSV files
\i /tmp/copy_commands.sql

-- Connect to the new database and create the schema
\c totesys_test_db
\i /tmp/schema.sql

-- Generate import commands for the new database
COPY (
    SELECT 
        'COPY ' || quote_ident(schemaname) || '.' || quote_ident(tablename) || ' FROM ' || quote_literal('/tmp/' || schemaname || '.' || tablename || '.csv') || ' WITH CSV HEADER;'
    FROM pg_tables 
    WHERE schemaname NOT IN ('pg_catalog', 'information_schema')
) TO '/tmp/import_commands.sql';

-- Execute the import commands to load data into the new database
\i /tmp/import_commands.sql
