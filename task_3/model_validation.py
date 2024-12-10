from google.cloud import bigquery

def validate_models(project, datasets):
    """
    Validates models in specified BigQuery project and datasets.
    
    Args:
        project (str): BigQuery project name.
        datasets (list): Datasets to validate.

    Returns:
        bool: True if all validations pass, False otherwise.
    """
    client = bigquery.Client(project=project)
    try:
        for dataset in datasets:
            for table in client.list_tables(dataset):
                table_id = table.table_id
                if not (
                    validate_schema(client, project, dataset, table_id) and
                    validate_row_count(client, project, dataset, table_id) and
                    validate_null_values(client, project, dataset, table_id)
                ):
                    print(f"Validation failed for table: {table_id}")
                    return False
        print("All validations passed.")
        return True
    except Exception as e:
        print(f"Validation error: {e}")
        return False

def validate_schema(client, project, dataset, table_id):
    """Ensures table schema matches expected fields."""
    expected_schema = {
        "orders": ["order_id", "amount", "currency", "order_status", "customer_id"],
        "customers": ["customer_id", "name", "company"]
    }
    if table_id in expected_schema:
        actual_fields = [field.name for field in client.get_table(f"{project}.{dataset}.{table_id}").schema]
        if not all(field in actual_fields for field in expected_schema[table_id]):
            print(f"Schema mismatch in table: {table_id}")
            return False
    return True

def validate_row_count(client, project, dataset, table_id):
    """Checks if the table is non-empty."""
    query = f"SELECT COUNT(*) AS row_count FROM `{project}.{dataset}.{table_id}`"
    row_count = list(client.query(query).result())[0].row_count
    if row_count == 0:
        print(f"Table {table_id} is empty.")
        return False
    return True

def validate_null_values(client, project, dataset, table_id):
    """Checks for nulls in critical fields."""
    critical_fields = {
        "orders": ["order_id", "amount", "currency"],
        "customers": ["customer_id", "name"]
    }
    if table_id in critical_fields:
        for field in critical_fields[table_id]:
            query = f"SELECT COUNT(*) AS null_count FROM `{project}.{dataset}.{table_id}` WHERE {field} IS NULL"
            null_count = list(client.query(query).result())[0].null_count
            if null_count > 0:
                print(f"Field '{field}' in {table_id} has {null_count} null values.")
                return False
    return True
