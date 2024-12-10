from model_validation import validate_models

def test_models_in_sandbox():
    # Run dbt transformations in the sandbox environment
    dbt_command = "dbt run --target sandbox"
    print(f"Executing: {dbt_command}")
    
    # Define the sandbox datasets
    sandbox_datasets = ["sandbox_models", "sandbox_intermediate"]

    # Validate models
    if validate_models("sandbox_project", sandbox_datasets):
        print("Sandbox models validated successfully.")
    else:
        print("Validation failed. Fix errors before proceeding.")


# Step 1: Define BigQuery Projects and Datasets
def setup_bigquery_projects():
    projects = {
        "raw_data_project": {
            "datasets": ["raw_orders", "raw_customers", "raw_shipments"]
        },
        "sandbox_project": {
            "datasets": ["sandbox_models", "sandbox_intermediate"]
        },
        "production_project": {
            "datasets": ["production_orders", "production_customers", "production_shipments"]
        }
    }
    print("BigQuery projects and datasets defined.")
    return projects

# Step 2: Load Raw Data into Raw Data Project
def load_raw_data(pipeline):
    pipeline.execute()
    print("Raw data loaded into 'raw_data_project' datasets.")

# Step 3: Develop and Test Models in Sandbox Project
def test_models_in_sandbox():
    # Run dbt transformations in sandbox environment
    dbt_command = "dbt run --target sandbox"
    print(f"Executing: {dbt_command}")
    
    # Validate models
    if validate_models("sandbox_project"):
        print("Sandbox models validated successfully.")
    else:
        print("Validation failed. Fix errors before proceeding.")


# Step 4: Deploy Validated Models to Production Project
def deploy_to_production():
    # Deploy models to production using dbt
    dbt_command = "dbt run --target production"
    print(f"Executing: {dbt_command}")
    
    # Grant access to production datasets
    grant_access("production_project", ["analysts", "visualization_tools"])
    print("Models deployed to production.")

# Step 5: Grant IAM Roles and Permissions
def grant_access(project, teams):
    access_rules = {
        "raw_data_project": {"Engineers": "BigQuery Data Editor"},
        "sandbox_project": {"Developers": "BigQuery Data Editor"},
        "production_project": {
            "Analysts": "BigQuery Data Viewer",
            "Visualization_Tools": "BigQuery Data Viewer",
            "Engineers": "BigQuery Job User"
        }
    }
    for team in teams:
        print(f"Granted {team} access to {project} as per IAM policy.")

# Step 6: Configure dbt for ELT Workflow
def configure_dbt_workflow():
    dbt_config = {
        "profiles_directory": "profiles/",
        "models_directory": "models/",
        "targets": {
            "sandbox": {"project": "sandbox_project", "datasets": "sandbox_*"},
            "production": {"project": "production_project", "datasets": "production_*"}
        }
    }
    print("dbt workflow configured.")
    return dbt_config

# Main Execution Flow
def main():
    print("Setting up BigQuery projects...")
    projects = setup_bigquery_projects()
    
    print("Loading raw data...")
    pipeline = DataPipeline(source_api="ERP_API", destination="raw_data_project")
    load_raw_data(pipeline)
    
    print("Testing models in sandbox...")
    test_models_in_sandbox()
    
    print("Deploying models to production...")
    deploy_to_production()
    
    print("Configuring IAM roles...")
    grant_access("raw_data_project", ["Engineers"])
    grant_access("sandbox_project", ["Developers"])
    grant_access("production_project", ["Analysts", "Visualization_Tools"])
    
    print("Pipeline and project setup complete.")

if __name__ == "__main__":
    main()
