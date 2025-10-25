"""
Worker Health Monitor DAG
Automatically checks worker availability every 5 minutes and updates registry
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import logging
import paramiko

logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'worker_health_monitor',
    default_args=default_args,
    description='Monitor worker availability and update registry status',
    schedule_interval='*/5 * * * *',  # Every 5 minutes
    catchup=False,
    tags=['monitoring', 'workers', 'health-check'],
)


def check_worker_health(**context):
    """
    Check SSH connectivity to all registered workers
    Update status in WORKER_REGISTRY
    """
    logger.info("=" * 80)
    logger.info("🏥 Worker Health Check - Starting")
    logger.info("=" * 80)
    
    # Load worker registry
    try:
        registry_json = Variable.get("WORKER_REGISTRY", default_var=None)
        if not registry_json:
            logger.warning("⚠️ WORKER_REGISTRY not found. No workers to check.")
            return {
                'status': 'no_registry',
                'message': 'WORKER_REGISTRY not configured'
            }
        
        registry = json.loads(registry_json) if isinstance(registry_json, str) else registry_json
        workers = registry.get('workers', {})
        
        if not workers:
            logger.warning("⚠️ No workers defined in WORKER_REGISTRY")
            return {
                'status': 'no_workers',
                'message': 'No workers configured'
            }
        
        logger.info(f"📋 Checking health of {len(workers)} worker(s)")
        
    except json.JSONDecodeError as e:
        logger.error(f"❌ Invalid WORKER_REGISTRY JSON: {e}")
        return {
            'status': 'error',
            'message': f'Invalid JSON: {e}'
        }
    
    # Check each worker
    results = {}
    # Try common SSH key locations
    import os
    ssh_key_path = None
    for key_file in ['/home/airflow/.ssh/id_ed25519', '/home/airflow/.ssh/id_rsa', 
                     '/home/airflow/.ssh/airflow_worker_key', '~/.ssh/id_ed25519', '~/.ssh/id_rsa']:
        expanded_path = os.path.expanduser(key_file)
        if os.path.exists(expanded_path):
            ssh_key_path = expanded_path
            logger.info(f"🔑 Using SSH key: {ssh_key_path}")
            break
    
    if not ssh_key_path:
        logger.warning("⚠️ No SSH key found. Will try default authentication.")
    
    updated = False
    
    for worker_name, worker_config in workers.items():
        logger.info(f"\n🔍 Checking worker: {worker_name}")
        logger.info(f"   Host: {worker_config.get('host')}")
        logger.info(f"   User: {worker_config.get('ssh_user')}")
        logger.info(f"   Current status: {worker_config.get('status', 'unknown')}")
        
        # Test SSH connection
        is_available = test_ssh_connection(
            host=worker_config.get('host'),
            ssh_user=worker_config.get('ssh_user'),
            ssh_port=worker_config.get('ssh_port', 22),
            ssh_key_path=ssh_key_path
        )
        
        new_status = 'available' if is_available else 'unavailable'
        old_status = worker_config.get('status', 'unknown')
        
        # Update status if changed
        if new_status != old_status:
            logger.info(f"   📝 Status changed: {old_status} → {new_status}")
            worker_config['status'] = new_status
            worker_config['last_checked'] = datetime.now().isoformat()
            updated = True
        else:
            logger.info(f"   ✓ Status unchanged: {new_status}")
            worker_config['last_checked'] = datetime.now().isoformat()
        
        results[worker_name] = {
            'status': new_status,
            'previous_status': old_status,
            'changed': new_status != old_status
        }
    
    # Save updated registry if any changes
    if updated:
        registry['workers'] = workers
        registry['last_updated'] = datetime.now().isoformat()
        
        try:
            Variable.set("WORKER_REGISTRY", json.dumps(registry))
            logger.info("\n💾 Worker registry updated successfully")
        except Exception as e:
            logger.error(f"\n❌ Failed to update registry: {e}")
            return {
                'status': 'error',
                'message': f'Failed to update registry: {e}',
                'results': results
            }
    else:
        logger.info("\n✓ No status changes detected")
    
    # Summary
    logger.info("\n" + "=" * 80)
    logger.info("📊 Health Check Summary:")
    logger.info("=" * 80)
    
    available_count = sum(1 for r in results.values() if r['status'] == 'available')
    unavailable_count = len(results) - available_count
    changed_count = sum(1 for r in results.values() if r['changed'])
    
    logger.info(f"Total workers: {len(results)}")
    logger.info(f"Available: {available_count}")
    logger.info(f"Unavailable: {unavailable_count}")
    logger.info(f"Status changes: {changed_count}")
    
    for worker_name, result in results.items():
        status_icon = "✅" if result['status'] == 'available' else "❌"
        change_icon = "🔄" if result['changed'] else "  "
        logger.info(f"  {status_icon} {change_icon} {worker_name}: {result['status']}")
    
    logger.info("=" * 80)
    
    return {
        'status': 'success',
        'total': len(results),
        'available': available_count,
        'unavailable': unavailable_count,
        'changes': changed_count,
        'results': results
    }


def test_ssh_connection(host: str, ssh_user: str, ssh_port: int, ssh_key_path: str = None, timeout: int = 10) -> bool:
    """
    Test SSH connectivity to a worker
    
    Returns:
        True if connection successful, False otherwise
    """
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        # Build connection parameters
        conn_params = {
            'hostname': host,
            'username': ssh_user,
            'port': ssh_port,
            'timeout': timeout,
            'banner_timeout': timeout
        }
        
        # Add key file only if it exists
        if ssh_key_path:
            conn_params['key_filename'] = ssh_key_path
        
        ssh.connect(**conn_params)
        
        # Test with simple command
        stdin, stdout, stderr = ssh.exec_command('echo "HEALTH_CHECK_OK"', timeout=5)
        result = stdout.read().decode('utf-8').strip()
        
        ssh.close()
        
        if "HEALTH_CHECK_OK" in result:
            logger.info(f"   ✅ SSH connection successful")
            return True
        else:
            logger.warning(f"   ⚠️ SSH connection established but unexpected response")
            return False
        
    except paramiko.AuthenticationException:
        logger.warning(f"   ❌ SSH authentication failed")
        return False
    except paramiko.SSHException as e:
        logger.warning(f"   ❌ SSH connection error: {e}")
        return False
    except TimeoutError:
        logger.warning(f"   ❌ SSH connection timeout")
        return False
    except Exception as e:
        logger.warning(f"   ❌ Connection failed: {e}")
        return False


def send_alert_if_needed(**context):
    """
    Send alert if critical workers are down
    Can be extended to send email, Slack, etc.
    """
    ti = context['ti']
    results = ti.xcom_pull(task_ids='check_worker_health')
    
    if not results or results.get('status') != 'success':
        logger.warning("⚠️ Health check did not complete successfully")
        return
    
    unavailable = results.get('unavailable', 0)
    total = results.get('total', 0)
    
    if unavailable > 0:
        logger.warning("=" * 80)
        logger.warning(f"⚠️ ALERT: {unavailable}/{total} workers are unavailable!")
        logger.warning("=" * 80)
        
        # TODO: Add email/Slack notifications here
        # Example:
        # send_slack_message(f"🚨 {unavailable} workers are down!")
        # send_email(subject="Worker Health Alert", body=...)
        
    else:
        logger.info(f"✅ All {total} workers are healthy")


# Define tasks
health_check_task = PythonOperator(
    task_id='check_worker_health',
    python_callable=check_worker_health,
    provide_context=True,
    dag=dag,
)

alert_task = PythonOperator(
    task_id='send_alert_if_needed',
    python_callable=send_alert_if_needed,
    provide_context=True,
    dag=dag,
)

# Task dependencies
health_check_task >> alert_task

