import os
import sys
import subprocess
import time
import json
from pathlib import Path
from dotenv import load_dotenv
import requests
import datetime

# Load .env file
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'), override=True)

# Allowed environments
VALID_ENVS = ["dev", "test", "prod", "p0c0"]

# Read environment variables
ENVIRONMENT = os.getenv("ENVIRONMENT")
SOURCE_DB_ID = os.getenv("SOURCE_DB_ID")
TARGET_DB_ID = os.getenv("TARGET_DB_ID")
TOKEN = os.getenv("ASTRA_TOKEN")

# Validate inputs
if not ENVIRONMENT or not SOURCE_DB_ID or not TARGET_DB_ID or not TOKEN:
    print("Error: Missing required environment variables in .env file.")

if ENVIRONMENT not in VALID_ENVS:
    print(f"Error: Invalid environment '{ENVIRONMENT}'. Allowed values are: {', '.join(VALID_ENVS)}.")

host_map = {
    "dev": "https://api.dev.cloud.datastax.com",
    "test": "https://api.test.cloud.datastax.com",
    "prod": "https://api.astra.datastax.com",
    "p0c0": "https://api.astra.datastax.com",
}

host = host_map[ENVIRONMENT]


def get_latest_snapshot_id(host, token, source_db_id):
    snapshots_url = f"{host}/v2/databases/{source_db_id}/snapshots"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(snapshots_url, headers=headers)
    if resp.status_code != 200:
        print(f"Error: Failed to get snapshots: {resp.text}", file=sys.stderr)
        sys.exit(1)
    snapshots = resp.json().get("snapshots", [])
    if not snapshots:
        print("Error: snapshotID is empty. No snapshots found for the specified source database.", file=sys.stderr)
        sys.exit(1)
    snapshotID = snapshots[-1]["id"]
    return snapshotID


def get_db_keyspaces(host, token, db_id):
    keyspaces_url = f"{host}/v2/databases/{db_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.get(keyspaces_url, headers=headers)
    if resp.status_code != 200:
        print(f"Error: Failed to get the keyspaces: {resp.text}", file=sys.stderr)
        sys.exit(1)
    try:
        keyspaces = resp.json()["info"]["keyspaces"]
    except Exception:
        print("Error: No keyspaces found. Exiting ...", file=sys.stderr)
        sys.exit(1)
    
    return keyspaces

def remove_db_keyspace(host, token, db_id, keyspace_name):
    remove_url = f"{host}/v2/databases/{db_id}/keyspaces/{keyspace_name}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.delete(remove_url, headers=headers)
    if resp.status_code >= 300:
        print(f"Error: Failed to remove the keyspace: {resp.text}", file=sys.stderr)
        return resp.status_code
    return f"Keyspace {keyspace_name} removed successfully. Status code: {resp.status_code} - {resp.text}"


def start_clone_operation(host, token, source_db_id, target_db_id, snapshot_id):
    clone_url = f"{host}/v2/databases/{target_db_id}/cloneFrom/{source_db_id}?snapshotID={snapshot_id}"
    headers = {"Authorization": f"Bearer {token}"}
    resp = requests.post(clone_url, headers=headers)
    print(resp.text)
    if resp.status_code != 200:
        print(f"Error: Failed to start clone operation: {resp.text}", file=sys.stderr)
        sys.exit(1)
    try:
        operationID = resp.json()["operationID"]
    except Exception:
        print("Error: operationID is empty. Something is wrong. Exiting ...", file=sys.stderr)
        sys.exit(1)

    try:
        operationID = resp.json()["operationID"]
    except Exception:
        print("Error: operationID is empty. Something is wrong. Exiting ...", file=sys.stderr)
        sys.exit(1)
    
    return operationID


def get_clone_status(host, token, source_db_id, operation_id):
    status_url = f"{host}/v2/databases/{source_db_id}/cloneStatus/{operation_id}"
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(status_url, headers=headers)
    try:
        resp_json = response.json()
    except Exception:
        print(f"Error: Invalid JSON response: {response.text}", file=sys.stderr)
        sys.exit(1)
    return resp_json


def monitor_clone_status(host, token, source_db_id, operation_id):
    status = ""
    print("\npress Ctrl+C to exit the script. Note: The clone job will still continue to run.")
    
    # Open the file once and keep it open throughout the monitoring
    with open(f"clone_{operation_id}.txt", "a") as f:
        while True:
            res = get_clone_status(host, token, source_db_id, operation_id)
            # print(json.dumps(res, indent=2))
            status = res.get("status")
            phase = res.get("phase")
            message = res.get("message")
            full_message = f"{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {phase} - {status} - {message}"
            f.write(full_message + "\n")
            f.flush()  # Ensure the data is written immediately
            print(full_message)
            
            if status == "Failed":
                print("Clone operation failed. Please check the logs for more details.")
                sys.exit(1)
            if status == "Completed":
                print("Clone operation completed successfully.")
                break

            time.sleep(15)
    return status

def help():
    print("Usage: python clone_for_astra_serverless.py <command>")
    print("Main Commands:")
    print("  start_clone - Start the clone operation")
    print("  monitor_clone_operation <operationID> - Monitor the clone operation")
    print("")
    print("If you want to execute it step by step, you can use the following commands:")
    print("")
    print("  get_latest_snapshot_id - Get the latest snapshot ID for the source database")
    print("  clone_operation_status <operationID> - Get the status of the clone operation")
    print("  destroy_target_db_keyspaces - Destroy the target database keyspaces")
    print("  get_target_db_keyspaces - Get the target database keyspaces")

if __name__ == "__main__":

    args = sys.argv[1:]
    
    try:
        args[0]
    except Exception:
        help()
        sys.exit(1)

    if args[0] == "get_latest_snapshot_id":
        snapshotID = get_latest_snapshot_id(host, TOKEN, SOURCE_DB_ID)
        print(snapshotID)
        sys.exit(0)
    elif args[0] == "get_target_db_keyspaces":
        keyspaces = get_db_keyspaces(host, TOKEN, TARGET_DB_ID)
        print(json.dumps(keyspaces, indent=2))
        sys.exit(0)
    elif args[0] == "destroy_target_db_keyspaces":
        keyspaces = get_db_keyspaces(host, TOKEN, TARGET_DB_ID)
        for keyspace in keyspaces:
            print(f"Removing keyspace: {keyspace}")
            resp = remove_db_keyspace(host, TOKEN, TARGET_DB_ID, keyspace)
            print(json.dumps(resp, indent=2))
        sys.exit(0)
    elif args[0] == "start_clone":
        snapshotID = get_latest_snapshot_id(host, TOKEN, SOURCE_DB_ID)
        print(f"Starting clone operation for snapshotID: {snapshotID} . Source DB ID: {SOURCE_DB_ID} . Target DB ID: {TARGET_DB_ID}")
        operationID = start_clone_operation(host, TOKEN, SOURCE_DB_ID, TARGET_DB_ID, snapshotID)
        print(f"Clone operation started successfully. OperationID: {operationID}")
        
        # Write the success message to a file
        success_message = f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Clone operation started successfully. OperationID: {operationID}"
        with open(f"clone_{operationID}.txt", "w") as f:
            f.write(success_message)
        
        print(f"OperationID: {operationID} is written to the file 'clone_{operationID}.txt'")
        print(f"Run 'python clone_for_astra_serverless.py monitor_clone_operation {operationID}' to monitor the clone operation.")
        sys.exit(0)
    elif args[0] == "clone_operation_status":
        operationID = args[1]
        print(f"Getting clone status for operationID: {operationID} . Target DB ID: {TARGET_DB_ID}")
        status = get_clone_status(host, TOKEN, TARGET_DB_ID, operationID)
        print(json.dumps(status, indent=2))
        sys.exit(0)
    elif args[0] == "monitor_clone_operation":
        operationID = args[1]
        print(f"Monitoring clone operation for operationID: {operationID} . Target DB ID: {TARGET_DB_ID}")
        status = monitor_clone_status(host, TOKEN, TARGET_DB_ID, operationID)
        print(json.dumps(status, indent=2))
        sys.exit(0)
    else:
        help()
