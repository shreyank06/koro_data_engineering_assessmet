### GCP Services Overview:
- Cloud Storage: To store raw and cleaned data.
- BigQuery: For data warehousing and analytics.
- Secret Manager: To securely store API keys.
- IAM: To enforce access control for the pipeline.

### Strategies to Validate Data Accuracy:
# 1. Validate schema consistency:
    - Ensure that the fields in each record match the expected schema.
    - Use the `validate_bigquery_schema` function to check for missing or extra fields.

# 2. Check data completeness:
    - Validate critical fields (e.g., `order_id`, `amount`) are non-null and logical.
    - Skip and log invalid records for further investigation using `log_data_issue`.

# 3. Data normalization:
    - Ensure consistent formats (e.g., uppercase currency codes) during transformation.

# 4. Anomaly detection:
    - Log unexpected values, like negative amounts or invalid order statuses.

### Strategies to Ensure Data Security:
# 1. Secure API keys:
    - Store API keys in GCP Secret Manager and retrieve dynamically.

# 2. Data encryption:
    - Use GCP’s default encryption for data at rest and in transit in Cloud Storage and BigQuery.

# 3. Role-based access control:
    - Use IAM roles to restrict access:
      - Service accounts with write access to Cloud Storage and BigQuery.
      - Read-only access for debugging or analytics purposes.

# 4. Audit logs:
    - Enable Cloud Audit Logs to monitor API usage and data access.
