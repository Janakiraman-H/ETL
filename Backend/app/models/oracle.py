# Replace the below functions with your actual OracleDB interaction logic
def execute_sql(sql_query):
    # Simulated execution of SQL query against OracleDB
    # Replace this with actual OracleDB query execution
    # Example: Use SQLAlchemy or other libraries to execute SQL
    return [
        {"id": 1, "name": "John Doe"},
        {"id": 2, "name": "Jane Smith"}
    ]

def get_column_names(result_data):
    # Simulated fetching of column names from query result
    # Replace this with actual logic to extract column names
    if result_data:
        return list(result_data[0].keys())
    return []
