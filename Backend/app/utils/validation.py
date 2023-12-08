def validate_sql(sql_query):
    # Add your SQL validation logic here
    # Example: Check if the query contains specific keywords, etc.
    # Simulated simple validation for demonstration purposes
    if "DELETE" in sql_query.upper():
        return False, "DELETE statements are not allowed"
    
    return True, ""
