# Databricks notebook source
import os
import great_expectations as gx

os.environ["GX_CLOUD_BASE_URL"] = "https://api.greatexpectations.io"

# COMMAND ----------

context = gx.get_context()
