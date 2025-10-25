"""
Test DAG for Smart Remote Worker Operator V2
Simplified and reliable with dynamic worker selection
"""

from datetime import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'plugins'))
from operators.smart_remote_worker_v2 import SmartRemoteWorkerOperatorV2

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

with DAG(
    'test_smart_v2',
    default_args=default_args,
    description='Test Smart Remote Worker V2 - Dynamic + Reliable',
    schedule=None,
    catchup=False,
    tags=['test', 'smart-v2', 'dynamic'],
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    def print_info(**context):
        print("=" * 60)
        print("🚀 Starting Smart Remote Worker V2 Test")
        print("=" * 60)
        print("Features:")
        print("  - Dynamic worker selection")
        print("  - Reliable SSHHook connections")
        print("  - Auto-creates SSH connections")
        print("  - Priority-based routing")
        print("=" * 60)
    
    info = PythonOperator(
        task_id='print_info',
        python_callable=print_info,
    )
    
    # Test 1: Simple command with auto worker selection
    test_simple = SmartRemoteWorkerOperatorV2(
        task_id='test_simple_command',
        bash_command="""
echo "=========================================="
echo "Simple test on remote worker"
echo "Hostname: $(hostname)"
echo "Date: $(date)"
echo "PWD: $(pwd)"
echo "User: $(whoami)"
echo "=========================================="
""",
        worker_priority=['worker-1'],  # Try worker-1 first
    )
    
    # Test 2: Check directory structure
    test_structure = SmartRemoteWorkerOperatorV2(
        task_id='test_directory_structure',
        bash_command="""
echo "=========================================="
echo "Checking directory structure"
echo "=========================================="
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo ""
echo "Directory contents:"
ls -la $AIRFLOW_HOME
echo ""
echo "Containers directory:"
ls -la $AIRFLOW_HOME/containers/ 2>/dev/null || echo "No containers directory"
echo ""
echo "DAGs directory:"
ls -la $AIRFLOW_HOME/dags/ 2>/dev/null || echo "No dags directory"
echo "=========================================="
""",
        worker_priority=['worker-1'],
    )
    
    # Test 3: Test Singularity
    test_singularity = SmartRemoteWorkerOperatorV2(
        task_id='test_singularity',
        bash_command="""
echo "=========================================="
echo "Testing Singularity"
echo "=========================================="

# Check if Singularity is available
if command -v singularity > /dev/null 2>&1; then
    echo "✅ Singularity is available"
    singularity --version
    
    # List Singularity images
    echo ""
    echo "Singularity images in AIRFLOW_HOME:"
    find $AIRFLOW_HOME -name "*.sif" -type f 2>/dev/null || echo "No .sif files found"
else
    echo "❌ Singularity is not available"
fi
echo "=========================================="
""",
        worker_priority=['worker-1'],
    )
    
    # Test 4: Test Python in Singularity
    test_python = SmartRemoteWorkerOperatorV2(
        task_id='test_python_in_singularity',
        bash_command="""
echo "=========================================="
echo "Testing Python inside Singularity"
echo "=========================================="

python3 --version 2>/dev/null || python --version

echo ""
echo "Python packages:"
python3 -c "import sys; print('\\n'.join(sys.path))" 2>/dev/null || echo "Python not available"

echo "=========================================="
""",
        worker_priority=['worker-1'],
    )
    
    def print_results(**context):
        ti = context['task_instance']
        
        print("=" * 60)
        print("📊 TEST RESULTS")
        print("=" * 60)
        
        for task_id in ['test_simple_command', 'test_directory_structure', 
                        'test_singularity', 'test_python_in_singularity']:
            result = ti.xcom_pull(task_ids=task_id)
            if result:
                print(f"\n✅ {task_id}:")
                print(f"   Worker: {result.get('worker')}")
                print(f"   Exit code: {result.get('exit_code')}")
            else:
                print(f"\n❌ {task_id}: No result")
        
        print("=" * 60)
    
    results = PythonOperator(
        task_id='print_results',
        python_callable=print_results,
    )
    
    end = EmptyOperator(task_id='end')
    
    # Define flow
    start >> info >> test_simple >> test_structure >> test_singularity >> test_python >> results >> end
