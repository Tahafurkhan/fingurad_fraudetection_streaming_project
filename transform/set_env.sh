#!/usr/bin/env bash
# Export the environment dbt needs. Usage:  source set_env.sh
#
# MSYS_NO_PATHCONV matters on Git Bash for Windows. Without it, MSYS rewrites
# any value starting with "/" into a Windows path, so DATABRICKS_HTTP_PATH
# arrives at dbt as
#   C:/Users/<you>/AppData/Local/Programs/Git/sql/1.0/warehouses/<id>
# and the connection fails with a bare HTTP 404 on OpenSession -- an error that
# says nothing about the real cause.

export MSYS_NO_PATHCONV=1

export DATABRICKS_HOST="dbc-66c31e7b-a4f0.cloud.databricks.com"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/b349348047ac54a2"

# Reuse the token already stored by `databricks configure --token` rather than
# keeping a second copy anywhere.
export DATABRICKS_TOKEN="$(python -c "
import configparser, os
c = configparser.ConfigParser()
c.read(os.path.expanduser('~/.databrickscfg'))
print(c['DEFAULT']['token'])
")"

echo "host      : $DATABRICKS_HOST"
echo "http_path : $DATABRICKS_HTTP_PATH"
echo "token     : ${#DATABRICKS_TOKEN} chars"
