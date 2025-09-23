#!/bin/bash

DB_USER="sql_dev"
DB_NAME="admin"

# Prompt for password once
read -s -p "Enter MySQL password: " DB_PASS
echo

# Loop through all .sql files in the current directory
for file in *.sql; do
    echo "Running $file..."
    mysql -u "$DB_USER" -p"$DB_PASS" "$DB_NAME" < "$file"
done

echo "All SQL scripts executed."
