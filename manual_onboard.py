# Databricks notebook source
# MAGIC %md
# MAGIC # DLT-META Onboarding Wrapper
# MAGIC 
# MAGIC This notebook receives parameters from Databricks Jobs and runs DLT-META onboarding.

# COMMAND ----------

# MAGIC %pip install dlt-meta

# COMMAND ----------

# Get parameters from job
dbutils.widgets.text("onboard_layer", "bronze")
dbutils.widgets.text("onboarding_file_path", "")
dbutils.widgets.text("database", "")
dbutils.widgets.text("bronze_dataflowspec_table", "bronze_dataflowspec")
dbutils.widgets.text("bronze_dataflowspec_path", "")
dbutils.widgets.text("silver_dataflowspec_table", "silver_dataflowspec")
dbutils.widgets.text("silver_dataflowspec_path", "")
dbutils.widgets.text("bronze_dataflowspec_table_ref", "")  # For silver onboarding reference
dbutils.widgets.text("overwrite", "True")
dbutils.widgets.text("env", "dev")
dbutils.widgets.text("version", "v1")

# Read parameters
onboard_layer = dbutils.widgets.get("onboard_layer")
onboarding_file_path = dbutils.widgets.get("onboarding_file_path")
database = dbutils.widgets.get("database")
bronze_dataflowspec_table = dbutils.widgets.get("bronze_dataflowspec_table")
bronze_dataflowspec_path = dbutils.widgets.get("bronze_dataflowspec_path")
silver_dataflowspec_table = dbutils.widgets.get("silver_dataflowspec_table")
silver_dataflowspec_path = dbutils.widgets.get("silver_dataflowspec_path")
bronze_dataflowspec_table_ref = dbutils.widgets.get("bronze_dataflowspec_table_ref")
overwrite = dbutils.widgets.get("overwrite")
env = dbutils.widgets.get("env")
version = dbutils.widgets.get("version")

print(f"🔧 Onboarding Parameters:")
print(f"   Layer: {onboard_layer}")
print(f"   File: {onboarding_file_path}")
print(f"   Database: {database}")

# COMMAND ----------

# Build onboarding parameters
onboarding_params_map = {
    "database": database,
    "onboarding_file_path": onboarding_file_path,
    "overwrite": overwrite,
    "env": env,
    "version": version
}

# Add layer-specific parameters
if onboard_layer == "bronze":
    onboarding_params_map["bronze_dataflowspec_table"] = bronze_dataflowspec_table
    if bronze_dataflowspec_path:
        onboarding_params_map["bronze_dataflowspec_path"] = bronze_dataflowspec_path
    print(f"   Bronze Spec Table: {bronze_dataflowspec_table}")
    
elif onboard_layer == "silver":
    onboarding_params_map["silver_dataflowspec_table"] = silver_dataflowspec_table
    if silver_dataflowspec_path:
        onboarding_params_map["silver_dataflowspec_path"] = silver_dataflowspec_path
    if bronze_dataflowspec_table_ref:
        onboarding_params_map["bronze_dataflowspec_table"] = bronze_dataflowspec_table_ref
    print(f"   Silver Spec Table: {silver_dataflowspec_table}")
    print(f"   Bronze Spec Table (ref): {bronze_dataflowspec_table_ref}")

# COMMAND ----------

# Run onboarding
from src.onboard_dataflowspec import OnboardDataflowspec

print(f"\n🚀 Starting {onboard_layer} onboarding...")
OnboardDataflowspec(spark, onboarding_params_map, uc_enabled=True).onboard_dataflow_specs()
print(f"\n✅ {onboard_layer.capitalize()} onboarding completed successfully!")
