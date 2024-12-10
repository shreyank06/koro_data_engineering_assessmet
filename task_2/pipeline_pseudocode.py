import requests
import json
from google.cloud import storage, bigquery
from google.cloud import secretmanager  # For secure API key storage


# Step 1: Extract Data from API
def extract_data_from_api(api_url, secret_name, bucket_name, raw_data_file):
    try:
        # Retrieve the API key securely from Secret Manager
        api_key = get_secret(secret_name)

        headers = {"Authorization": f"Bearer {api_key}"}
        response = requests.get(api_url, headers=headers)
        if response.status_code == 200:
            save_to_cloud_storage(bucket_name, raw_data_file, response.json())
            print("Data extraction successful.")
        else:
            print(f"API call failed: {response.status_code}")
    except Exception as e:
        print(f"Error during data extraction: {e}")

# Step 2: Transform Data
def transform_data(bucket_name, raw_data_file, cleaned_data_file):
    try:
        raw_data = read_from_cloud_storage(bucket_name, raw_data_file)
        cleaned_data = []
        for record in raw_data:
            if is_valid_record(record):
                cleaned_data.append(transform_record(record))
            else:
                log_data_issue(record, "Invalid record detected")
        save_to_cloud_storage(bucket_name, cleaned_data_file, cleaned_data)
        print("Data transformation successful.")
    except Exception as e:
        print(f"Error during data transformation: {e}")

def is_valid_record(record):
    # Validate data accuracy during transformation
    return record.get("order_id") is not None and record.get("amount", 0) >= 0

def transform_record(record):
    record["currency"] = record["currency"].upper()  # Normalize currency format
    return record

# Step 3: Load Data into BigQuery
def load_data_into_bigquery(bucket_name, cleaned_data_file, bigquery_table):
    try:
        client = bigquery.Client()
        data = read_from_cloud_storage(bucket_name, cleaned_data_file)

        # Validate JSON structure matches BigQuery schema
        if not validate_bigquery_schema(data):
            raise ValueError("Data schema does not match BigQuery table schema.")

        job = client.load_table_from_json(data, bigquery_table)
        job.result()  # Wait for the load job to complete
        print("Data loaded into BigQuery successfully.")
    except Exception as e:
        print(f"Error during data loading: {e}")

def validate_bigquery_schema(data):
    # Example schema validation logic
    required_fields = ["order_id", "amount", "currency"]
    for record in data:
        for field in required_fields:
            if field not in record:
                log_data_issue(record, f"Missing field: {field}")
                return False
    return True

# Helper Functions
def save_to_cloud_storage(bucket_name, file_name, data):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(data))
    print(f"Saved {file_name} to Cloud Storage.")

def read_from_cloud_storage(bucket_name, file_name):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    data = blob.download_as_string()
    return json.loads(data)

def get_secret(secret_name):
    # Retrieve secret from GCP Secret Manager
    client = secretmanager.SecretManagerServiceClient()
    secret_path = f"projects/YOUR_PROJECT_ID/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(name=secret_path)
    return response.payload.data.decode("UTF-8")

def log_data_issue(record, message):
    # Logs data quality issues
    print(f"Data Quality Issue: {message} | Record: {record}")

# Main Pipeline Execution
def run_pipeline():
    api_url = "https://erp-system-api.com/data"
    secret_name = "erp_api_key"  # Secret Manager key name
    bucket_name = "your-cloud-storage-bucket"
    raw_data_file = "raw_data.json"
    cleaned_data_file = "cleaned_data.json"
    bigquery_table = "your_project.your_dataset.Fact_Orders"

    try:
        extract_data_from_api(api_url, secret_name, bucket_name, raw_data_file)
        transform_data(bucket_name, raw_data_file, cleaned_data_file)
        load_data_into_bigquery(bucket_name, cleaned_data_file, bigquery_table)
        print("Pipeline executed successfully.")
    except Exception as e:
        print(f"Pipeline execution failed: {e}")

if __name__ == "__main__":
    run_pipeline()


### GCP Services Overview:
# - Cloud Storage: To store raw and cleaned data.
# - BigQuery: For data warehousing and analytics.
# - Secret Manager: To securely store API keys.
# - IAM: To enforce access control for the pipeline.

### Strategies to Validate Data Accuracy:
# 1. Validate schema consistency:
#    - Ensure that the fields in each record match the expected schema.
#    - Use the `validate_bigquery_schema` function to check for missing or extra fields.
#
# 2. Check data completeness:
#    - Validate critical fields (e.g., `order_id`, `amount`) are non-null and logical.
#    - Skip and log invalid records for further investigation using `log_data_issue`.
#
# 3. Data normalization:
#    - Ensure consistent formats (e.g., uppercase currency codes) during transformation.
#
# 4. Anomaly detection:
#    - Log unexpected values, like negative amounts or invalid order statuses.

### Strategies to Ensure Data Security:
# 1. Secure API keys:
#    - Store API keys in GCP Secret Manager and retrieve dynamically.
#
# 2. Data encryption:
#    - Use GCP’s default encryption for data at rest and in transit in Cloud Storage and BigQuery.
#
# 3. Role-based access control:
#    - Use IAM roles to restrict access:
#      - Service accounts with write access to Cloud Storage and BigQuery.
#      - Read-only access for debugging or analytics purposes.
#
# 4. Audit logs:
#    - Enable Cloud Audit Logs to monitor API usage and data access.
