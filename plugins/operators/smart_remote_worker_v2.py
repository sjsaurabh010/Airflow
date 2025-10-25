"""
Smart Remote Worker Operator V2 - Simplified and Reliable
Combines dynamic worker selection with reliable SSHHook execution
"""

from airflow.models import BaseOperator, Variable, Connection
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.exceptions import AirflowException
from airflow import settings
from typing import Optional, List, Dict, Any
import json
import time
import logging

logger = logging.getLogger(__name__)


class SmartRemoteWorkerOperatorV2(BaseOperator):
    """
    Smart operator with:
    - Dynamic worker selection from WORKER_REGISTRY
    - Priority-based routing
    - Reliable SSHHook connections
    - Automatic SSH connection creation
    """
    
    template_fields = ['bash_command']
    
    def __init__(
        self,
        bash_command: str,
        worker_priority: Optional[List[str]] = None,
        retry_delay_seconds: int = 300,
        queue: str = 'default',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.bash_command = bash_command
        self.worker_priority = worker_priority or []
        self.retry_delay_seconds = retry_delay_seconds
        self.queue = queue
    
    def get_worker_registry(self) -> Dict:
        """Load worker registry from Airflow Variables"""
        try:
            registry_json = Variable.get("WORKER_REGISTRY", default_var=None)
            if not registry_json:
                raise AirflowException("WORKER_REGISTRY not found in Airflow Variables")
            
            registry = json.loads(registry_json) if isinstance(registry_json, str) else registry_json
            workers = registry.get('workers', {})
            
            if not workers:
                raise AirflowException("No workers defined in WORKER_REGISTRY")
            
            logger.info(f"📋 Loaded {len(workers)} workers from registry")
            return workers
            
        except json.JSONDecodeError as e:
            raise AirflowException(f"Invalid WORKER_REGISTRY JSON: {e}")
    
    def select_workers(self, registry: Dict) -> List[tuple]:
        """
        Select workers based on priority list
        Returns: [(worker_name, worker_config), ...]
        """
        selected = []
        
        # If priority list specified, use it
        if self.worker_priority:
            for worker_name in self.worker_priority:
                if worker_name in registry:
                    config = registry[worker_name]
                    if config.get('status') == 'available':
                        selected.append((worker_name, config))
        else:
            # Use all available workers sorted by priority
            available = [
                (name, config) for name, config in registry.items()
                if config.get('status') == 'available'
            ]
            selected = sorted(available, key=lambda x: x[1].get('priority', 999))
        
        if not selected:
            raise AirflowException(
                f"No available workers found. "
                f"Priority list: {self.worker_priority}, "
                f"Registry has {len(registry)} workers"
            )
        
        return selected
    
    def ensure_ssh_connection(self, worker_name: str, worker_config: Dict) -> str:
        """
        Ensure SSH connection exists in Airflow for this worker
        Returns: connection_id
        """
        conn_id = f"worker_{worker_name}"
        session = settings.Session()
        
        try:
            # Check if connection exists
            existing = session.query(Connection).filter(
                Connection.conn_id == conn_id
            ).first()
            
            if not existing:
                # Create new connection
                new_conn = Connection(
                    conn_id=conn_id,
                    conn_type='ssh',
                    host=worker_config['host'],
                    login=worker_config.get('ssh_user', 'airflow'),
                    port=worker_config.get('ssh_port', 22),
                    extra=json.dumps({
                        'no_host_key_check': True,
                        'timeout': 30
                    })
                )
                session.add(new_conn)
                session.commit()
                logger.info(f"✅ Created SSH connection: {conn_id}")
            else:
                logger.info(f"✅ Using existing SSH connection: {conn_id}")
            
            return conn_id
            
        finally:
            session.close()
    
    def build_worker_command(self, worker_name: str, worker_config: Dict) -> str:
        """Build command to start worker and execute task"""
        
        airflow_home = worker_config.get('airflow_home', '/home/airflow/airflow-worker')
        
        command = f"""#!/bin/bash
set -x

# Setup environment
export AIRFLOW_HOME={airflow_home}
export WORKER_NAME={worker_name}
export QUEUE_NAME={self.queue}

echo "====================================="
echo "Starting worker: $WORKER_NAME"
echo "AIRFLOW_HOME: $AIRFLOW_HOME"
echo "====================================="

# Go to Airflow directory
cd $AIRFLOW_HOME || exit 1

# Check for Singularity container
SINGULARITY_IMAGE=""
if [ -f "containers/airflow-worker.sif" ]; then
    SINGULARITY_IMAGE="containers/airflow-worker.sif"
elif [ -f "airflow-worker.sif" ]; then
    SINGULARITY_IMAGE="airflow-worker.sif"
elif [ -f "airflow_2.10.2.sif" ]; then
    SINGULARITY_IMAGE="airflow_2.10.2.sif"
fi

if [ -n "$SINGULARITY_IMAGE" ]; then
    echo "Found Singularity image: $SINGULARITY_IMAGE"
    
    # Test if Singularity works
    if singularity --version > /dev/null 2>&1; then
        echo "Singularity is available"
        
        # Execute the bash command in Singularity container
        echo "Executing command in Singularity container..."
        singularity exec --bind $AIRFLOW_HOME:$AIRFLOW_HOME $SINGULARITY_IMAGE bash -c '''
{self.bash_command}
'''
        exit_code=$?
        echo "Command completed with exit code: $exit_code"
        exit $exit_code
    else
        echo "Singularity not available, executing directly"
        {self.bash_command}
        exit $?
    fi
else
    echo "No Singularity image found, executing directly"
    {self.bash_command}
    exit $?
fi
"""
        return command
    
    def execute_on_worker(self, worker_name: str, worker_config: Dict) -> Any:
        """Execute task on selected worker"""
        
        logger.info(f"🎯 Executing on worker: {worker_name}")
        logger.info(f"   Host: {worker_config['host']}")
        logger.info(f"   AIRFLOW_HOME: {worker_config.get('airflow_home', '/home/airflow/airflow-worker')}")
        
        try:
            # Ensure SSH connection exists
            conn_id = self.ensure_ssh_connection(worker_name, worker_config)
            
            # Get SSH hook
            ssh_hook = SSHHook(ssh_conn_id=conn_id)
            
            # Build command
            command = self.build_worker_command(worker_name, worker_config)
            
            logger.info("📤 Executing command via SSH...")
            logger.info(f"Command:\n{command[:500]}...")
            
            # Execute command
            ssh_client = ssh_hook.get_conn()
            stdin, stdout, stderr = ssh_client.exec_command(command, timeout=600)
            
            # Read output
            output = stdout.read().decode('utf-8')
            error = stderr.read().decode('utf-8')
            exit_status = stdout.channel.recv_exit_status()
            
            # Log results
            logger.info(f"📊 Exit status: {exit_status}")
            
            if output:
                logger.info(f"📄 Output:\n{output}")
            
            if error:
                logger.info(f"⚠️ Stderr:\n{error}")
            
            ssh_client.close()
            
            if exit_status != 0:
                raise AirflowException(
                    f"Command failed with exit code {exit_status}\n"
                    f"Output: {output}\n"
                    f"Error: {error}"
                )
            
            logger.info(f"✅ Task completed successfully on {worker_name}")
            
            return {
                'worker': worker_name,
                'exit_code': exit_status,
                'output': output,
                'error': error
            }
            
        except Exception as e:
            logger.error(f"❌ Failed to execute on {worker_name}: {str(e)}")
            raise
    
    def execute(self, context: Dict[str, Any]) -> Any:
        """Main execution method"""
        
        logger.info("=" * 80)
        logger.info("🚀 Smart Remote Worker Operator V2 - Starting")
        logger.info("=" * 80)
        
        # Load worker registry
        registry = self.get_worker_registry()
        
        # Select workers
        selected_workers = self.select_workers(registry)
        
        logger.info(f"📋 Selected {len(selected_workers)} worker(s)")
        for worker_name, config in selected_workers:
            logger.info(f"   - {worker_name} (priority: {config.get('priority', 'N/A')}, host: {config['host']})")
        
        # Try each worker
        last_error = None
        
        for attempt, (worker_name, worker_config) in enumerate(selected_workers, 1):
            logger.info("=" * 80)
            logger.info(f"🎯 ATTEMPT {attempt}/{len(selected_workers)}: Trying worker '{worker_name}'")
            logger.info("=" * 80)
            
            try:
                result = self.execute_on_worker(worker_name, worker_config)
                
                logger.info("=" * 80)
                logger.info("✅ SUCCESS: Task completed")
                logger.info("=" * 80)
                
                return result
                
            except Exception as e:
                last_error = e
                logger.warning(f"⚠️ Worker '{worker_name}' failed: {str(e)}")
                
                # If more workers available, try next one
                if attempt < len(selected_workers):
                    logger.info(f"⏭️ Trying next worker...")
                    continue
                else:
                    # No more workers, check if we should retry
                    if self.retry_delay_seconds > 0 and attempt == 1:
                        logger.info(f"⏳ Retrying after {self.retry_delay_seconds} seconds...")
                        time.sleep(self.retry_delay_seconds)
                        
                        # Retry same worker
                        try:
                            result = self.execute_on_worker(worker_name, worker_config)
                            logger.info("✅ SUCCESS on retry")
                            return result
                        except Exception as retry_error:
                            last_error = retry_error
                            logger.error(f"❌ Retry also failed: {str(retry_error)}")
        
        # All workers failed
        logger.error("=" * 80)
        logger.error("❌ FAILURE: All workers failed")
        logger.error("=" * 80)
        
        raise AirflowException(
            f"Task failed on all available workers. "
            f"Tried {len(selected_workers)} worker(s): {[w[0] for w in selected_workers]}. "
            f"Last error: {str(last_error)}"
        )

