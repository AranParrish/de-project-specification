def format_data(result: list, db: str) -> dict | list:
    """
    Takes an input query result and database connection and returns
    in a format suitable for writing to a JSON file.
    For a single row query result, returns as a dictionary of key-value pairs
    with column headings as the keys.
    For multiple row query results, returns a list of dictionary key-value pairs.
    Args:
        result:
            list representing the output from a pg8000 query of a database
        db:
            str representing the database connection used to return the query result
            (required to get column headings)
    
    Returns:
        Single dictionary:
            For single row query result with key-value pairs using table column headings as keys
        List of dictionaries:
            For multiple row query results with key-value pairs in each dictionary using column headings as keys
    """
    col_headers = [col["name"] for col in db.columns]
    if len(result) == 1:
        return dict(zip(col_headers, result[0]))
    return [dict(zip(col_headers, data)) for data in result]