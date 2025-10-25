#!/bin/bash

# Fix the full_integration_test.py file
# This converts Windows line endings to Unix line endings

echo "Fixing full_integration_test.py line endings..."

# Check if dos2unix is available
if command -v dos2unix &> /dev/null; then
    dos2unix ~/airflow/dags/full_integration_test.py
    echo "✅ Fixed using dos2unix"
elif command -v sed &> /dev/null; then
    # Use sed to remove carriage returns
    sed -i 's/\r$//' ~/airflow/dags/full_integration_test.py
    echo "✅ Fixed using sed"
else
    # Use tr command
    tr -d '\r' < ~/airflow/dags/full_integration_test.py > /tmp/full_integration_test_fixed.py
    mv /tmp/full_integration_test_fixed.py ~/airflow/dags/full_integration_test.py
    echo "✅ Fixed using tr"
fi

echo ""
echo "Verifying the DAG..."
python3 -m py_compile ~/airflow/dags/full_integration_test.py

if [ $? -eq 0 ]; then
    echo "✅ DAG syntax is now correct!"
    echo ""
    echo "Test it with:"
    echo "  airflow dags list | grep full_integration_test"
else
    echo "❌ Still has issues, checking for errors..."
    python3 ~/airflow/dags/full_integration_test.py
fi

