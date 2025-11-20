#!/bin/bash

set -e

# Configuration
DB_NAME="edr"
DB_USER="postgres"
DB_HOST="localhost"
DB_PASSWORD="changeMe"

# File names
SQL_GZ_FILE="edr_backup.sql.gz"
CUSTOM_FILE="edr_backup.dump"

export PGPASSWORD="$DB_PASSWORD"

echo "===== Dumping as SQL.gz ====="
START_TIME=$(date +%s)
pg_dump -h $DB_HOST -U $DB_USER -d $DB_NAME -F p -v | gzip > $SQL_GZ_FILE
END_TIME=$(date +%s)
SQL_DUMP_TIME=$((END_TIME - START_TIME))
echo "SQL.gz dump completed in $SQL_DUMP_TIME seconds."

echo
echo "===== Dumping as custom format ====="
START_TIME=$(date +%s)
pg_dump -h $DB_HOST -U $DB_USER -d $DB_NAME -F c -b -v -f $CUSTOM_FILE
END_TIME=$(date +%s)
CUSTOM_DUMP_TIME=$((END_TIME - START_TIME))
echo "Custom format dump completed in $CUSTOM_DUMP_TIME seconds."

echo
echo "===== Restoring SQL.gz ====="
# Create test database for restore
psql -h $DB_HOST -U $DB_USER -c "DROP DATABASE IF EXISTS ${DB_NAME}_sqlgz;"
psql -h $DB_HOST -U $DB_USER -c "CREATE DATABASE ${DB_NAME}_sqlgz;"
START_TIME=$(date +%s)
gunzip -c $SQL_GZ_FILE | psql -h $DB_HOST -U $DB_USER -d ${DB_NAME}_sqlgz
END_TIME=$(date +%s)
SQL_RESTORE_TIME=$((END_TIME - START_TIME))
echo "SQL.gz restore completed in $SQL_RESTORE_TIME seconds."

echo
echo "===== Restoring custom format ====="
# Create test database for restore
psql -h $DB_HOST -U $DB_USER -c "DROP DATABASE IF EXISTS ${DB_NAME}_custom;"
psql -h $DB_HOST -U $DB_USER -c "CREATE DATABASE ${DB_NAME}_custom;"
START_TIME=$(date +%s)
pg_restore -h $DB_HOST -U $DB_USER -d ${DB_NAME}_custom -v $CUSTOM_FILE
END_TIME=$(date +%s)
CUSTOM_RESTORE_TIME=$((END_TIME - START_TIME))
echo "Custom format restore completed in $CUSTOM_RESTORE_TIME seconds."

echo
echo "===== Summary ====="
echo "SQL.gz Dump Time: $SQL_DUMP_TIME sec"
echo "SQL.gz Restore Time: $SQL_RESTORE_TIME sec"
echo "Custom Dump Time: $CUSTOM_DUMP_TIME sec"
echo "Custom Restore Time: $CUSTOM_RESTORE_TIME sec"
