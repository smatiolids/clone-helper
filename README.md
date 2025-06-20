# Self-Service Clone Restore

Copy the env_sample to .env and update the values.

## How to run

```
python3 clone_for_astra_serverless.py <command>
```

#### Commands

```
# Start the clone operation
python3 clone_for_astra_serverless.py start_clone 

# Get the status of the clone operation
python3 clone_for_astra_serverless.py clone_operation_status <operationID> 

# Monitor the clone operation - Continue monitoring the clone operation until it is complete
python3 clone_for_astra_serverless.py monitor_clone_operation <operationID> 

# Get the latest snapshot ID for the source database
python3 clone_for_astra_serverless.py get_latest_snapshot_id 

# Destroy the target database keyspaces
python3 clone_for_astra_serverless.py destroy_target_db_keyspaces 

# Get the target database keyspaces
python3 clone_for_astra_serverless.py get_target_db_keyspaces 
``` 