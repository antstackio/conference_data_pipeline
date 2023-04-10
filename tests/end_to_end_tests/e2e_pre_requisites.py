# Databricks notebook source
# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE conference_raw CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE conference_refined CASCADE

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC DROP DATABASE conference_trusted CASCADE

# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS conference_raw

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS conference_refined

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS conference_trusted
