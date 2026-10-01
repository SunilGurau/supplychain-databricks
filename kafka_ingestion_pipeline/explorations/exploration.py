# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
ca_path = "/Volumes/operations-catalog/supplychain/kafka-client-certificates/ca.pem"

with open(ca_path, "r") as f:
    ca_contents = f.read()

print(ca_contents)

# COMMAND ----------

# MAGIC %sql
# MAGIC select 1;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM `operations-catalog`.supplychain.bronze_purchase_order;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)
# MAGIC FROM `operations-catalog`.supplychain.bronze_purchase_order_lines;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from `operations-catalog`.supplychain.silver_purchase_order