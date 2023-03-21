# Databricks notebook source
# MAGIC %sh
# MAGIC 
# MAGIC cd /tmp || exit
# MAGIC echo "Downloading sonar-scanner....."
# MAGIC if [ -d "/tmp/sonar-scanner-cli-4.7.0.2747-linux.zip" ];then
# MAGIC     sudo rm /tmp/sonar-scanner-cli-4.7.0.2747-linux.zip
# MAGIC fi
# MAGIC wget -q https://binaries.sonarsource.com/Distribution/sonar-scanner-cli/sonar-scanner-cli-4.7.0.2747-linux.zip
# MAGIC echo "Download completed."
# MAGIC 
# MAGIC echo "Unziping downloaded file..."
# MAGIC unzip sonar-scanner-cli-4.7.0.2747-linux.zip
# MAGIC echo "Unzip completed."
# MAGIC rm sonar-scanner-cli-4.7.0.2747-linux.zip
# MAGIC 
# MAGIC echo "Installing to opt..."
# MAGIC if [ -d "/var/opt/sonar-scanner-4.7.0.2747-linux" ];then
# MAGIC     sudo rm -rf /var/opt/sonar-scanner-4.7.0.2747-linux
# MAGIC fi
# MAGIC sudo mv sonar-scanner-4.7.0.2747-linux /var/opt
# MAGIC 
# MAGIC echo "Installation completed successfully."
# MAGIC 
# MAGIC echo "You can use sonar-scanner!"
# MAGIC 
# MAGIC export PATH="/var/opt/sonar-scanner-4.7.0.2747-linux/bin:$PATH"

# COMMAND ----------

import pytest
import os
import sys

repo_name = "conference_data_pipeline"

# Get the path to this notebook, for example "/Workspace/Repos/{username}/{repo-name}".
notebook_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

# Get the repo's root directory name.
repo_root = os.path.dirname(os.path.dirname(notebook_path))

# Prepare to run pytest from the repo.
os.chdir(f"/Workspace{repo_root}/")
print(os.getcwd())

# Skip writing pyc files on a readonly filesystem.
sys.dont_write_bytecode = True

# Run pytest.
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider", "--cov","--cov-report","term","--cov-report", f"xml:{os.getcwd()}/coverage-reports/coverage.xml"])

# Fail the cell execution if there are any test failures.
assert retcode == 0, "The pytest invocation failed. See the log for details."

# COMMAND ----------

# MAGIC %sh
# MAGIC 
# MAGIC sh /var/opt/sonar-scanner-4.7.0.2747-linux/bin/sonar-scanner -X  \
# MAGIC   -Dsonar.projectKey=antstackio_conference_data_pipeline \
# MAGIC   -Dsonar.organization=antstackio \
# MAGIC   -Dsonar.host.url=https://sonarcloud.io \
# MAGIC   -Dsonar.python.coverage.reportPaths=coverage-reports/coverage.xml \
# MAGIC   -Dsonar.sources=. \
# MAGIC   -Dsonar.tests=tests/ \
# MAGIC   -Dsonar.pullrequest.key=`cat dbfs:/tmp/databricks-github-actions/arg-pr-key.txt` \
# MAGIC   -Dsonar.login=`cat dbfs:/tmp/databricks-github-actions/arg-sonar-token.txt` 
