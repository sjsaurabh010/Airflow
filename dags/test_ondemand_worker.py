"""
Test DAG for ON-DEMAND Worker Lifecycle
Tests: Start Worker → Execute Task → Stop Worker
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

# Import our custom operators
from operators.remote_worker_operator import (
    RemoteWorkerStartOperator,
    RemoteWorkerStopOperator,
    WorkerHealthCheckSensor
)


default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='test_ondemand_worker',
    default_args=default_args,
    description='Test ON-DEMAND worker lifecycle',
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['test', 'ondemand', 'worker'],
) as dag:
    
    # Step 1: Start remote worker
    start_worker = RemoteWorkerStartOperator(
        task_id='start_remote_worker',
        ssh_conn_id='remote_worker_ssh',
        worker_name='worker1',
        queue_name='worker1',
        health_check_timeout=300,
        health_check_interval=10
    )
    
    # Step 2: Health check
    health_check = WorkerHealthCheckSensor(
        task_id='worker_health_check',
        worker_name='worker1',
        poke_interval=60,
        timeout=300
    )
    
    # Step 3: Simple test task (runs on worker)
    @task(queue='worker1')
    def test_worker_execution():
        """
        Simple task that executes on remote worker.
        Tests that task routing works correctly.
        """
        import socket
        import os
        
        hostname = socket.gethostname()
        username = os.getenv('USER')
        
        print(f"✅ Task executing on: {hostname}")
        print(f"   Username: {username}")
        print(f"   This proves task ran on REMOTE worker!")
        
        return {
            'hostname': hostname,
            'username': username,
            'message': 'Task executed successfully on remote worker'
        }
    
    test_task = test_worker_execution()
    
    # Step 4: Stop remote worker
    stop_worker = RemoteWorkerStopOperator(
        task_id='stop_remote_worker',
        ssh_conn_id='remote_worker_ssh',
        worker_name='worker1',
        verify_stopped=True,
        trigger_rule='all_done'  # Run even if test_task failed
    )
    
    # Define dependencies
    start_worker >> health_check >> test_task >> stop_worker


if __name__ == "__main__":
    dag.test()
