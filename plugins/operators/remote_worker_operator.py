"""
Remote Worker Management Operators
Implements ON-DEMAND worker lifecycle management

Client Requirement: Workers start ON-DEMAND only (no persistent processes)
"""

import time
import subprocess
from typing import Optional
from airflow.models import BaseOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from airflow.exceptions import AirflowException


class RemoteWorkerStartOperator(BaseOperator):
    """
    Starts a remote Celery worker via SSH.
    Waits for worker to register with Master before returning.
    
    Client Requirement: "Workflow: Start worker → Execute DAG → Stop worker"
    """
    
    template_fields = ['worker_name', 'queue_name']
    ui_color = '#90EE90'  # Light green
    
    def __init__(
        self,
        ssh_conn_id: str,
        worker_name: str,
        queue_name: str,
        start_script_path: str = "~/scripts/start-worker.sh",
        health_check_timeout: int = 300,  # 5 minutes
        health_check_interval: int = 10,  # Check every 10 seconds
        **kwargs
    ):
        """
        Args:
            ssh_conn_id: Airflow Connection ID for SSH to remote
            worker_name: Unique worker identifier (e.g., 'worker1')
            queue_name: Celery queue name (usually same as worker_name)
            start_script_path: Path to worker startup script on remote
            health_check_timeout: Max seconds to wait for registration
            health_check_interval: Seconds between health checks
        """
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.worker_name = worker_name
        self.queue_name = queue_name
        self.start_script_path = start_script_path
        self.health_check_timeout = health_check_timeout
        self.health_check_interval = health_check_interval
    
    def execute(self, context):
        """Main execution logic"""
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        
        self.log.info(f"🚀 Starting remote worker: {self.worker_name}")
        self.log.info(f"   Queue: {self.queue_name}")
        self.log.info(f"   Script: {self.start_script_path}")
        
        # Start worker in background (nohup)
        start_command = f"nohup bash {self.start_script_path} > /tmp/worker_{self.worker_name}_start.log 2>&1 &"
        
        with ssh_hook.get_conn() as ssh_client:
            self.log.info("Executing worker start command via SSH...")
            stdin, stdout, stderr = ssh_client.exec_command(start_command)
            exit_code = stdout.channel.recv_exit_status()
            
            if exit_code != 0:
                error = stderr.read().decode()
                raise AirflowException(f"❌ Failed to start worker: {error}")
            
            self.log.info("✅ Worker start command sent successfully")
        
        # Wait for worker to register
        self.log.info(f"⏳ Waiting for worker registration (timeout: {self.health_check_timeout}s)...")
        
        start_time = time.time()
        attempts = 0
        
        while time.time() - start_time < self.health_check_timeout:
            attempts += 1
            
            if self._check_worker_registered():
                elapsed = int(time.time() - start_time)
                self.log.info(f"✅ Worker '{self.worker_name}' registered successfully!")
                self.log.info(f"   Registration took: {elapsed} seconds ({attempts} attempts)")
                
                return {
                    "worker_name": self.worker_name,
                    "queue_name": self.queue_name,
                    "registration_time": elapsed,
                    "status": "registered"
                }
            
            self.log.info(
                f"   Attempt {attempts}: Worker not registered yet. "
                f"Retrying in {self.health_check_interval}s..."
            )
            time.sleep(self.health_check_interval)
        
        # Timeout reached
        raise AirflowException(
            f"❌ Worker '{self.worker_name}' did not register within "
            f"{self.health_check_timeout}s ({attempts} attempts)"
        )
    
    def _check_worker_registered(self) -> bool:
        """
        Check if worker is registered with Celery via inspect command.
        Returns True if worker found, False otherwise.
        """
        try:
            result = subprocess.run(
                [
                    "celery", "-A", "airflow.executors.celery_executor",
                    "inspect", "active", "--destination", f"{self.worker_name}@*"
                ],
                capture_output=True,
                text=True,
                timeout=10,
                env={"PATH": "/home/airflow/airflow-env/bin:/usr/local/bin:/usr/bin:/bin"}
            )
            
            # Check if worker name appears in output
            return self.worker_name in result.stdout
            
        except subprocess.TimeoutExpired:
            self.log.warning("Health check timed out")
            return False
        except Exception as e:
            self.log.warning(f"Health check failed: {e}")
            return False


class RemoteWorkerStopOperator(BaseOperator):
    """
    Stops a remote Celery worker via SSH.
    Performs graceful shutdown and verifies cleanup.
    
    Client Requirement: "Workers must start ON-DEMAND only (no persistent processes)"
    """
    
    template_fields = ['worker_name']
    ui_color = '#FFB6C1'  # Light red
    
    def __init__(
        self,
        ssh_conn_id: str,
        worker_name: str,
        stop_script_path: str = "~/scripts/stop-worker.sh",
        graceful_timeout: int = 300,  # 5 minutes for graceful shutdown
        verify_stopped: bool = True,
        **kwargs
    ):
        """
        Args:
            ssh_conn_id: Airflow Connection ID for SSH to remote
            worker_name: Worker identifier to stop
            stop_script_path: Path to worker stop script on remote
            graceful_timeout: Max seconds to wait for graceful shutdown
            verify_stopped: Whether to verify worker actually stopped
        """
        super().__init__(**kwargs)
        self.ssh_conn_id = ssh_conn_id
        self.worker_name = worker_name
        self.stop_script_path = stop_script_path
        self.graceful_timeout = graceful_timeout
        self.verify_stopped = verify_stopped
    
    def execute(self, context):
        """Main execution logic"""
        ssh_hook = SSHHook(ssh_conn_id=self.ssh_conn_id)
        
        self.log.info(f"🛑 Stopping remote worker: {self.worker_name}")
        
        stop_command = f"bash {self.stop_script_path}"
        
        with ssh_hook.get_conn() as ssh_client:
            self.log.info("Executing worker stop command via SSH...")
            stdin, stdout, stderr = ssh_client.exec_command(
                stop_command, 
                timeout=self.graceful_timeout
            )
            
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            
            if exit_code != 0:
                error = stderr.read().decode()
                self.log.warning(f"⚠️  Stop script returned non-zero exit code: {error}")
            else:
                self.log.info(f"Stop script output:\n{output}")
        
        # Verify worker stopped
        if self.verify_stopped:
            self.log.info("🔍 Verifying worker stopped...")
            time.sleep(5)  # Give processes time to terminate
            
            if self._check_worker_registered():
                self.log.warning(
                    f"⚠️  Worker '{self.worker_name}' still registered after stop command!"
                )
            else:
                self.log.info(f"✅ Worker '{self.worker_name}' stopped successfully")
        
        return {
            "worker_name": self.worker_name,
            "status": "stopped"
        }
    
    def _check_worker_registered(self) -> bool:
        """Check if worker is still registered"""
        try:
            result = subprocess.run(
                [
                    "celery", "-A", "airflow.executors.celery_executor",
                    "inspect", "active", "--destination", f"{self.worker_name}@*"
                ],
                capture_output=True,
                text=True,
                timeout=10,
                env={"PATH": "/home/airflow/airflow-env/bin:/usr/local/bin:/usr/bin:/bin"}
            )
            return self.worker_name in result.stdout
        except:
            return False


class WorkerHealthCheckSensor(BaseOperator):
    """
    Periodically checks if worker is still alive and responsive.
    Used during long-running task execution to detect worker failures.
    """
    
    ui_color = '#FFD700'  # Gold
    
    def __init__(
        self,
        worker_name: str,
        poke_interval: int = 60,  # Check every minute
        timeout: int = 300,  # 5 minutes timeout
        **kwargs
    ):
        """
        Args:
            worker_name: Worker identifier to monitor
            poke_interval: Seconds between health checks
            timeout: Max seconds before considering worker dead
        """
        super().__init__(**kwargs)
        self.worker_name = worker_name
        self.poke_interval = poke_interval
        self.timeout = timeout
    
    def execute(self, context):
        """Main execution logic"""
        self.log.info(f"💓 Starting health check for worker: {self.worker_name}")
        
        start_time = time.time()
        checks = 0
        
        while time.time() - start_time < self.timeout:
            checks += 1
            
            if self._check_worker_alive():
                self.log.info(f"✅ Worker '{self.worker_name}' is responsive (check #{checks})")
                return True
            
            self.log.warning(
                f"⚠️  Worker '{self.worker_name}' not responding (check #{checks}). "
                f"Retrying in {self.poke_interval}s..."
            )
            time.sleep(self.poke_interval)
        
        raise AirflowException(
            f"❌ Worker '{self.worker_name}' not responsive for {self.timeout}s ({checks} checks)"
        )
    
    def _check_worker_alive(self) -> bool:
        """Check if worker is alive via Celery ping"""
        try:
            result = subprocess.run(
                [
                    "celery", "-A", "airflow.executors.celery_executor",
                    "inspect", "ping", "--destination", f"{self.worker_name}@*"
                ],
                capture_output=True,
                text=True,
                timeout=10,
                env={"PATH": "/home/airflow/airflow-env/bin:/usr/local/bin:/usr/bin:/bin"}
            )
            
            # Worker must respond with 'pong'
            return "pong" in result.stdout.lower()
            
        except:
            return False
