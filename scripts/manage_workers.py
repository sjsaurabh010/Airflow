#!/usr/bin/env python3
"""
Worker Management CLI Tool
Manage remote workers without touching code - all via Airflow Variables
"""

import argparse
import json
import sys
import os
from datetime import datetime
from typing import Dict, Optional

# Try to import Airflow modules
try:
    from airflow.models import Variable
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False
    print("⚠️ Warning: Airflow not available. Using direct database access mode.")


class WorkerManager:
    """Manage worker registry via Airflow Variables"""
    
    REGISTRY_KEY = "WORKER_REGISTRY"
    
    def __init__(self):
        if not AIRFLOW_AVAILABLE:
            print("❌ Error: Airflow is not installed or not in PYTHONPATH")
            print("Please activate Airflow virtual environment first:")
            print("  source /home/airflow/airflow/venv/bin/activate")
            sys.exit(1)
    
    def get_registry(self) -> Dict:
        """Get current worker registry"""
        try:
            registry_json = Variable.get(self.REGISTRY_KEY, default_var=None)
            
            if not registry_json:
                # Initialize empty registry
                return {
                    'workers': {},
                    'last_updated': datetime.now().isoformat()
                }
            
            registry = json.loads(registry_json) if isinstance(registry_json, str) else registry_json
            
            # Ensure workers key exists
            if 'workers' not in registry:
                registry['workers'] = {}
            
            return registry
            
        except json.JSONDecodeError as e:
            print(f"❌ Error: Invalid JSON in WORKER_REGISTRY: {e}")
            sys.exit(1)
    
    def save_registry(self, registry: Dict):
        """Save updated registry"""
        try:
            registry['last_updated'] = datetime.now().isoformat()
            Variable.set(self.REGISTRY_KEY, json.dumps(registry, indent=2))
            print("💾 Registry saved successfully")
        except Exception as e:
            print(f"❌ Error saving registry: {e}")
            sys.exit(1)
    
    def list_workers(self):
        """List all workers"""
        registry = self.get_registry()
        workers = registry.get('workers', {})
        
        if not workers:
            print("\n📋 No workers configured")
            print("\nTo add a worker, use:")
            print("  python manage_workers.py add WORKER_ID IP_ADDRESS --user USERNAME")
            return
        
        print("\n" + "=" * 90)
        print("📋 WORKER REGISTRY")
        print("=" * 90)
        print(f"Last updated: {registry.get('last_updated', 'N/A')}")
        print()
        
        # Header
        print(f"{'Worker ID':<20} {'Host':<20} {'Status':<12} {'Priority':<8} {'Container':<12} {'User':<10}")
        print("-" * 90)
        
        # Sort by priority
        sorted_workers = sorted(
            workers.items(),
            key=lambda x: x[1].get('priority', 999)
        )
        
        for worker_id, config in sorted_workers:
            status = config.get('status', 'unknown')
            status_icon = "✅" if status == 'available' else "❌"
            
            print(
                f"{worker_id:<20} "
                f"{config.get('host', 'N/A'):<20} "
                f"{status_icon} {status:<9} "
                f"{config.get('priority', 'N/A'):<8} "
                f"{config.get('container_type', 'N/A'):<12} "
                f"{config.get('ssh_user', 'N/A'):<10}"
            )
        
        print("=" * 90)
        print(f"Total workers: {len(workers)}")
        print(f"Available: {sum(1 for w in workers.values() if w.get('status') == 'available')}")
        print(f"Unavailable: {sum(1 for w in workers.values() if w.get('status') == 'unavailable')}")
        print()
    
    def add_worker(
        self,
        worker_id: str,
        host: str,
        ssh_user: str,
        priority: int = 10,
        ssh_port: int = 22,
        container_type: str = 'singularity',
        tags: Optional[list] = None
    ):
        """Add or update a worker"""
        registry = self.get_registry()
        workers = registry['workers']
        
        # Check if worker already exists
        if worker_id in workers:
            print(f"⚠️ Warning: Worker '{worker_id}' already exists. Updating...")
        
        # Create worker config
        worker_config = {
            'host': host,
            'ssh_user': ssh_user,
            'ssh_port': ssh_port,
            'priority': priority,
            'status': 'available',  # Default to available
            'container_type': container_type,
            'tags': tags or [],
            'added_date': datetime.now().isoformat()
        }
        
        workers[worker_id] = worker_config
        registry['workers'] = workers
        
        # Save
        self.save_registry(registry)
        
        print(f"\n✅ Worker '{worker_id}' added successfully!")
        print("\nConfiguration:")
        print(f"  Host: {host}")
        print(f"  User: {ssh_user}")
        print(f"  Port: {ssh_port}")
        print(f"  Priority: {priority}")
        print(f"  Container: {container_type}")
        print(f"  Status: available")
        
        print("\n📝 Next steps:")
        print(f"  1. Setup SSH access: python scripts/setup_ssh_keys.sh {host} {ssh_user}")
        print(f"  2. Deploy worker launcher script to {host}")
        print(f"  3. Test worker with: airflow dags trigger example_dag_with_workers")
    
    def remove_worker(self, worker_id: str):
        """Remove a worker"""
        registry = self.get_registry()
        workers = registry['workers']
        
        if worker_id not in workers:
            print(f"❌ Error: Worker '{worker_id}' not found")
            sys.exit(1)
        
        # Get worker info before deletion
        worker_info = workers[worker_id]
        
        # Remove
        del workers[worker_id]
        registry['workers'] = workers
        
        # Save
        self.save_registry(registry)
        
        print(f"\n✅ Worker '{worker_id}' removed successfully")
        print(f"   Host: {worker_info.get('host')}")
        print(f"   User: {worker_info.get('ssh_user')}")
    
    def set_status(self, worker_id: str, status: str):
        """Set worker status"""
        if status not in ['available', 'unavailable']:
            print("❌ Error: Status must be 'available' or 'unavailable'")
            sys.exit(1)
        
        registry = self.get_registry()
        workers = registry['workers']
        
        if worker_id not in workers:
            print(f"❌ Error: Worker '{worker_id}' not found")
            sys.exit(1)
        
        old_status = workers[worker_id].get('status', 'unknown')
        workers[worker_id]['status'] = status
        workers[worker_id]['status_updated'] = datetime.now().isoformat()
        
        registry['workers'] = workers
        self.save_registry(registry)
        
        print(f"\n✅ Worker '{worker_id}' status updated")
        print(f"   Old status: {old_status}")
        print(f"   New status: {status}")
    
    def set_priority(self, worker_id: str, priority: int):
        """Set worker priority"""
        registry = self.get_registry()
        workers = registry['workers']
        
        if worker_id not in workers:
            print(f"❌ Error: Worker '{worker_id}' not found")
            sys.exit(1)
        
        old_priority = workers[worker_id].get('priority', 'N/A')
        workers[worker_id]['priority'] = priority
        
        registry['workers'] = workers
        self.save_registry(registry)
        
        print(f"\n✅ Worker '{worker_id}' priority updated")
        print(f"   Old priority: {old_priority}")
        print(f"   New priority: {priority}")
    
    def show_worker(self, worker_id: str):
        """Show detailed worker information"""
        registry = self.get_registry()
        workers = registry['workers']
        
        if worker_id not in workers:
            print(f"❌ Error: Worker '{worker_id}' not found")
            sys.exit(1)
        
        worker = workers[worker_id]
        
        print("\n" + "=" * 60)
        print(f"📋 WORKER DETAILS: {worker_id}")
        print("=" * 60)
        
        status_icon = "✅" if worker.get('status') == 'available' else "❌"
        
        print(f"\nConnection:")
        print(f"  Host:          {worker.get('host')}")
        print(f"  User:          {worker.get('ssh_user')}")
        print(f"  Port:          {worker.get('ssh_port', 22)}")
        
        print(f"\nConfiguration:")
        print(f"  Status:        {status_icon} {worker.get('status', 'unknown')}")
        print(f"  Priority:      {worker.get('priority', 'N/A')}")
        print(f"  Container:     {worker.get('container_type', 'N/A')}")
        print(f"  Tags:          {', '.join(worker.get('tags', [])) or 'None'}")
        
        print(f"\nTimestamps:")
        print(f"  Added:         {worker.get('added_date', 'N/A')}")
        print(f"  Last checked:  {worker.get('last_checked', 'N/A')}")
        print(f"  Status update: {worker.get('status_updated', 'N/A')}")
        
        print("=" * 60)


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description='Manage Airflow remote workers',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all workers
  python manage_workers.py list
  
  # Add a new worker
  python manage_workers.py add worker-1 45.151.155.74 --user airflow --priority 1
  
  # Add worker with custom configuration
  python manage_workers.py add worker-2 10.0.1.20 \\
      --user airflow --priority 2 --container singularity --tags gpu,high-mem
  
  # Remove a worker
  python manage_workers.py remove worker-1
  
  # Set worker status (for maintenance)
  python manage_workers.py set-status worker-1 unavailable
  
  # Change worker priority
  python manage_workers.py set-priority worker-1 5
  
  # Show worker details
  python manage_workers.py show worker-1
        """
    )
    
    subparsers = parser.add_subparsers(dest='command', help='Command to execute')
    
    # List command
    subparsers.add_parser('list', help='List all workers')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add or update a worker')
    add_parser.add_argument('worker_id', help='Worker ID (e.g., worker-1)')
    add_parser.add_argument('host', help='Worker host IP or hostname')
    add_parser.add_argument('--user', dest='ssh_user', required=True, help='SSH username')
    add_parser.add_argument('--port', dest='ssh_port', type=int, default=22, help='SSH port (default: 22)')
    add_parser.add_argument('--priority', type=int, default=10, help='Worker priority (lower = higher priority, default: 10)')
    add_parser.add_argument('--container', dest='container_type', choices=['singularity', 'venv'], default='singularity', help='Container type (default: singularity)')
    add_parser.add_argument('--tags', nargs='+', help='Worker tags (e.g., gpu high-memory)')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a worker')
    remove_parser.add_argument('worker_id', help='Worker ID to remove')
    
    # Set status command
    status_parser = subparsers.add_parser('set-status', help='Set worker status')
    status_parser.add_argument('worker_id', help='Worker ID')
    status_parser.add_argument('status', choices=['available', 'unavailable'], help='New status')
    
    # Set priority command
    priority_parser = subparsers.add_parser('set-priority', help='Set worker priority')
    priority_parser.add_argument('worker_id', help='Worker ID')
    priority_parser.add_argument('priority', type=int, help='New priority (lower = higher)')
    
    # Show command
    show_parser = subparsers.add_parser('show', help='Show worker details')
    show_parser.add_argument('worker_id', help='Worker ID')
    
    # Parse arguments
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        sys.exit(0)
    
    # Initialize manager
    manager = WorkerManager()
    
    # Execute command
    if args.command == 'list':
        manager.list_workers()
    
    elif args.command == 'add':
        manager.add_worker(
            worker_id=args.worker_id,
            host=args.host,
            ssh_user=args.ssh_user,
            ssh_port=args.ssh_port,
            priority=args.priority,
            container_type=args.container_type,
            tags=args.tags
        )
    
    elif args.command == 'remove':
        manager.remove_worker(args.worker_id)
    
    elif args.command == 'set-status':
        manager.set_status(args.worker_id, args.status)
    
    elif args.command == 'set-priority':
        manager.set_priority(args.worker_id, args.priority)
    
    elif args.command == 'show':
        manager.show_worker(args.worker_id)


if __name__ == '__main__':
    main()
