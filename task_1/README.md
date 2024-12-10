Task 1: Schema Design and Implementation

Documentation:

### 1. Schema Overview:
This schema is designed using a star schema approach for analytics in BigQuery.
It includes one Fact table (Fact_Orders) and two Dimension tables (Dim_Customer, Dim_Address).
The design ensures efficiency, scalability, and usability for e-commerce data analytics.

### 2. Key Design Decisions:
- Star schema simplifies querying and supports analytics use cases.
- Fact table holds transactional data, and dimension tables hold descriptive data.
- Foreign key relationships establish clear links between tables.

### 3. Advantages:
- Easy to write aggregation-heavy queries for analytics.
- Clear separation of facts and dimensions improves readability and maintainability.
- Optimized for analytical queries in BigQuery due to its columnar storage.

### 4. Disadvantages:
- Requires ETL processes to maintain dimension table consistency.
- Joining large tables may increase query cost in BigQuery.

### 5. Considerations:
- Data correctness: Ensure all foreign key constraints are met and avoid null values.
- Anomalies: Use validation checks for duplicate, missing, or inconsistent data.
- Incremental loads: Use a robust ETL pipeline for incremental updates.

### 6. Common Analytics Queries:
- Tracking revenue trends by country and currency.
- Analyzing customer activity and order history.
- Monitoring shipping performance and identifying delays.
