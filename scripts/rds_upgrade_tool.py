import boto3
import os
import sys
import logging
import threading
from botocore.exceptions import ClientError
from packaging import version
from datetime import datetime
import argparse
import time
import scripts.check_pg_slots_and_extensions as check_pg_slots_and_extensions

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger(__name__)

def parse_arguments():
    """Function to parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Upgrade RDS or Aurora instance.")
    
    # Mandatory arguments
    parser.add_argument("-i", "--identifier", required=True, help="The unique identifier for the RDS or Aurora instance (e.g., 'mydbinstance').")
    parser.add_argument("-t", "--target-version", required=True, help="The target database engine version to upgrade to (e.g., '15.8').")

    # # Optional arguments
    # parser.add_argument("--delete-bg", action="store_true", help="Flag to delete the Blue-Green deployment after the upgrade. Use this if you want to remove the Blue-Green deployment post-upgrade.")
    # parser.add_argument("--delete-rds", action="store_true", help="Flag to delete the RDS or Aurora instance itself after the upgrade. Use this if you want to remove the instance post-upgrade.")

    return parser.parse_args()

def initialize_aws_clients():
    """Validates AWS environment variables and initializes AWS clients."""
    required_env_vars = ["AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN"]
    
    # Validate required environment variables
    missing_vars = [var for var in required_env_vars if var not in os.environ]
    if missing_vars:
        logger.error(f"Missing required environment variables: {', '.join(missing_vars)}")
        sys.exit(1)

    # Initialize clients and verify credentials
    try:
        aws_region = os.environ["AWS_REGION"]
        rds_client = boto3.client('rds', region_name=aws_region)
        account_id = boto3.client('sts', region_name=aws_region).get_caller_identity()['Account']
        logger.info(f"AWS account number: {account_id}")
        return rds_client
    except Exception as e:
        logger.error(f"Error initializing AWS clients: {e}")
        sys.exit(1)

def validate_rds_or_aurora(rds_client, identifier):
    """
    Validates whether the given identifier corresponds to an RDS PostgreSQL instance or an Aurora cluster.
    Returns a tuple: (instance details, instance type) or (None, None) if not found.
    """
    logger.info(f"Processing RDS instance upgrade: {identifier}")
    logger.info("==============================================")
    try:
        # Try to describe the DB cluster first (Aurora)
        response = rds_client.describe_db_clusters(DBClusterIdentifier=identifier)
        db_clusters = response.get('DBClusters', [])

        if db_clusters:
            engine = db_clusters[0].get('Engine', '')
            if 'aurora' in engine.lower():  # Check for any aurora engine type
                logger.info(f"Aurora cluster '{identifier}' exists with engine '{engine}'.")
                return db_clusters[0], 'Aurora'

    except rds_client.exceptions.DBClusterNotFoundFault:
        # If not an Aurora cluster, check for RDS instance
        pass  # Continue to the next block for RDS check
    
    try:
        # Check for a traditional RDS instance (PostgreSQL or others)
        response = rds_client.describe_db_instances(DBInstanceIdentifier=identifier)
        db_instances = response.get('DBInstances', [])

        if db_instances:
            engine = db_instances[0].get('Engine', '')
            if engine == 'postgres':
                logger.info(f"RDS PostgreSQL instance '{identifier}' exists with engine '{engine}'.")
                return db_instances[0], 'RDS'

    except rds_client.exceptions.DBInstanceNotFoundFault:
        pass  # No RDS instance found
    except Exception as e:
        logger.error(f"Error while checking instance: {e}")

    # If no matches found for either Aurora or RDS instance
    logger.error(f"Identifier '{identifier}' does not match any RDS or Aurora instance.")
    sys.exit(1)

def validate_versions(current_version, target_version):
    current_version_parsed = version.parse(current_version)
    target_version_parsed = version.parse(target_version)

    if current_version_parsed == target_version_parsed:
        logger.info(f"Current version {current_version} matches the target version {target_version}. No upgrade required.")
    elif current_version_parsed < target_version_parsed:
        logger.info(f"Current version {current_version} is older than the target version {target_version}. Upgrade is required.")
        return True  # Indicating that an upgrade is needed
    else:
        logger.error(f"Current version {current_version} is newer than the target version {target_version}. Downgrade is not supported.")
    return False  # Indicating no upgrade is needed

def get_blue_green_deployment_identifier(rds_client, instance_identifier):
    try:
        #logger.info("Searching for Blue/Green deployments...")
        response = rds_client.describe_blue_green_deployments()
        
        # Iterate through the deployments to find the one associated with the instance
        for deployment in response.get('BlueGreenDeployments', []):
            source_arn = deployment.get('Source', '')
            target_arn = deployment.get('Target', '')
            bg_identifier = deployment.get('BlueGreenDeploymentIdentifier', '')

            if instance_identifier in source_arn or instance_identifier in target_arn:
                #logger.info(f"Found Blue/Green deployment '{bg_identifier}' for instance '{instance_identifier}'.")
                return bg_identifier

        logger.warning(f"No Blue/Green deployment found for instance '{instance_identifier}'.")
        return None

    except Exception as e:
        logger.error(f"Error occurred while finding Blue/Green deployment: {e}")
        return None

def check_blue_green_deployment_status(rds_client, deployment_id, bg_identifier):
    try:
        logger.info(f"Checking status of Blue/Green deployment '{deployment_id}' with identifier '{bg_identifier}'...")

        # Describe Blue/Green deployments
        response = rds_client.describe_blue_green_deployments(
            BlueGreenDeploymentIdentifier=bg_identifier
        )
        
        deployments = response.get('BlueGreenDeployments', [])
        if deployments:
            # Extract the status and identifier
            deployment = deployments[0]
            status = deployment.get('Status', 'UNKNOWN')
            logger.info(f"Status of deployment '{deployment_id}': {status}")
            
            # Return the status along with the bg_identifier
            return status
        else:
            logger.warning(f"No Blue/Green deployment found with identifier '{deployment_id}'.")
            return None

    except Exception as e:
        logger.error(f"Error occurred while checking status for '{deployment_id}': {e}")
        return None

def initiate_blue_green_upgrade(rds_client, identifier, db_engine_version, instance_type):
    try:
        logger.info("No active Blue/Green deployment for the instance. Proceeding with upgrade.")
        
        # Describe the DB instance or cluster
        if instance_type == 'RDS':
            response = rds_client.describe_db_instances(DBInstanceIdentifier=identifier)
            db_instance_arn = response['DBInstances'][0]['DBInstanceArn']
        elif instance_type == 'Aurora':
            response = rds_client.describe_db_clusters(DBClusterIdentifier=identifier)
            db_instance_arn = response['DBClusters'][0]['DBClusterArn']
        else:
            logger.error("Unsupported instance type.")
            return False
        
        logger.info(f"DB instance '{identifier}' exists.")
        logger.info(f"DB instance ARN: {db_instance_arn}")
        
        # Identify if backup retention period is set to at least 1 day
        if instance_type == 'RDS':
            # Fetch DB instance details
            response = rds_client.describe_db_instances(DBInstanceIdentifier=identifier)
            backup_retention_period = response['DBInstances'][0].get('BackupRetentionPeriod', 0)
        elif instance_type == 'Aurora':
            # Fetch DB cluster details
            response = rds_client.describe_db_clusters(DBClusterIdentifier=identifier)
            backup_retention_period = response['DBClusters'][0].get('BackupRetentionPeriod', 0)

        if backup_retention_period < 1:
            logger.info(f"Backup retention period for '{identifier}' is less than 1 day. Modifying to 1 day.")
            
            if instance_type == 'RDS':
                # Modify the DB instance to set the backup retention period to 1 day
                response = rds_client.modify_db_instance(
                    DBInstanceIdentifier=identifier,
                    BackupRetentionPeriod=1,  # Set the number of days for backup retention (minimum 1 day)
                    ApplyImmediately=True
                )
                # Wait for the DB instance to be available
                waiter = rds_client.get_waiter('db_instance_available')
                waiter.wait(DBInstanceIdentifier=identifier)
                
            elif instance_type == 'Aurora':
                # Modify the DB cluster to set the backup retention period to 1 day
                response = rds_client.modify_db_cluster(
                    DBClusterIdentifier=identifier,
                    BackupRetentionPeriod=1,  # Set the number of days for backup retention (minimum 1 day)
                    ApplyImmediately=True
                )
                # Wait for the DB cluster to be available
                waiter = rds_client.get_waiter('db_cluster_available')
                waiter.wait(DBClusterIdentifier=identifier)
            
            logger.info(f"Backup retention period for '{identifier}' set to 1 day.")
        else:
            logger.info(f"Backup retention period for '{identifier}' is already set to {backup_retention_period} day(s).")

        
        # Set Blue/Green deployment name
        max_length = 60 - len("bg-deployment-")
        blue_green_deployment_name = f"bg-deployment-{identifier[:max_length]}"

        # Create a Blue/Green deployment
        response = rds_client.create_blue_green_deployment(
            BlueGreenDeploymentName=blue_green_deployment_name,  # Deployment name
            Source=db_instance_arn,  # Primary DB instance ARN (Blue)
            TargetEngineVersion=db_engine_version,  # Target DB engine version
        )
        logger.info(f"Blue/Green deployment created successfully: {response['BlueGreenDeployment']['BlueGreenDeploymentIdentifier']}")
        
        # store this db bg unique identifier to a variable
        bg_identifier = {response['BlueGreenDeployment']['BlueGreenDeploymentIdentifier']}
        return bg_identifier  # Indicating successful deployment creation and returning the deployment name

    except ClientError as e:
        logger.error(f"Failed to create Blue/Green deployment: {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return False

def switchover_blue_green_deployment(rds_client, bg_identifier):
    try:
        logger.info(f"Initiating switchover for Blue/Green deployment '{bg_identifier}'...")
        rds_client.switchover_blue_green_deployment(
            BlueGreenDeploymentIdentifier=bg_identifier,
            SwitchoverTimeout=300
        )
        status = 'SWITCHOVER_IN_PROGRESS'
        logger.info(f"Switchover initiated for Blue/Green deployment '{bg_identifier}'.")
        return status
    except ClientError as e:
        logger.error(f"Failed to initiate switchover for Blue/Green deployment '{bg_identifier}': {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return False

def delete_blue_green_deployment(rds_client, bg_identifier, database_name):
    try:
        logger.info("Switchover completed. Proceeding with deletion of Blue/Green deployment.")
        
        # # Ask user for confirmation before deleting the Blue/Green deployment
        # confirm_deployment = timeout_input(f"Are you sure you want to delete the Blue/Green deployment? (yes/no): ", 60)
        # logger.info("confirm_deployment: %s", confirm_deployment)
        
        # if confirm_deployment.lower() != 'yes':
        #     logger.info("Deletion of Blue/Green deployment cancelled.")
        #     return None

        # Delete Blue/Green deployment
        response = rds_client.delete_blue_green_deployment(
            BlueGreenDeploymentIdentifier=bg_identifier
        )
        logger.info("Blue/Green deployment deletion response: %s", response)

        # Extract source ARN and database name
        source_arn = response['BlueGreenDeployment']['Source']
        database_name = source_arn.split(':')[-1].split('/')[-1]
        logger.info(f"Database Name: {database_name}")

        # Return the extracted database name
        return database_name

    except rds_client.exceptions.ClientError as e:
        logger.error(f"Failed to delete Blue/Green deployment: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return None

def delete_database_instance_or_cluster(rds_client, instance_type, database_name):
    try:
        # # Confirm deletion with the user
        # confirm_deletion = timeout_input(f"Are you sure you want to delete the {instance_type} '{database_name}'? (yes/no): ", 60)
        # if confirm_deletion.lower() != 'yes':
        #     logger.info(f"Deletion of {instance_type} '{database_name}' cancelled.")
        #     return False

        if instance_type == 'RDS':
            # Disable deletion protection for RDS instance
            logger.info(f"Disabling deletion protection for RDS instance '{database_name}'...")
            rds_client.modify_db_instance(
                DBInstanceIdentifier=database_name,
                DeletionProtection=False
            )
            logger.info(f"Deletion protection disabled for RDS instance '{database_name}'.")

            # Delete RDS instance
            logger.info(f"Deleting RDS instance '{database_name}'...")
            response = rds_client.delete_db_instance(
                DBInstanceIdentifier=database_name,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=False
            )
            logger.info(f"Deletion initiated for RDS instance '{database_name}' in Blue/Green deployment.")

        elif instance_type == 'Aurora':
            # Disable deletion protection for Aurora cluster
            logger.info(f"Disabling deletion protection for Aurora cluster '{database_name}'...")
            rds_client.modify_db_cluster(
                DBClusterIdentifier=database_name,
                DeletionProtection=False
            )
            logger.info(f"Deletion protection disabled for Aurora cluster '{database_name}'.")

            # List and delete DB instances in the Aurora cluster
            logger.info(f"Listing DB instances in Aurora cluster '{database_name}'...")
            instances = rds_client.describe_db_instances(
                Filters=[{'Name': 'db-cluster-id', 'Values': [database_name]}]
            )['DBInstances']

            for instance in instances:
                instance_id = instance['DBInstanceIdentifier']
                logger.info(f"Deleting DB instance '{instance_id}' in Aurora cluster '{database_name}'...")
                rds_client.delete_db_instance(
                    DBInstanceIdentifier=instance_id,
                    SkipFinalSnapshot=True,
                    DeleteAutomatedBackups=False
                )
                # logger.info(f"Waiting for DB instance '{instance_id}' to be deleted...")
                # waiter = rds_client.get_waiter('db_instance_deleted')
                # waiter.wait(DBInstanceIdentifier=instance_id)
                logger.info(f"DB instance '{instance_id}' successfully deleted.")

            # Delete Aurora cluster
            logger.info(f"Deleting Aurora cluster '{database_name}'...")
            response = rds_client.delete_db_cluster(
                DBClusterIdentifier=database_name,
                SkipFinalSnapshot=True,
                DeleteAutomatedBackups=False
            )
            # waiter = rds_client.get_waiter('db_cluster_deleted')
            # waiter.wait(DBClusterIdentifier=database_name)
            logger.info(f"Deletion initiated for Aurora cluster '{database_name}' in Blue/Green deployment.")

        else:
            logger.error("Invalid instance type provided. Supported types are 'RDS' or 'Aurora'.")
            return False

        return True

    except rds_client.exceptions.ClientError as e:
        logger.error(f"Failed to delete {instance_type} '{database_name}': {e}")
        return False
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return False

def create_snapshot(rds_client, identifier, instance_type):
    try:
        # Ask user for confirmation before creating snapshot of the instance, default is no
        confirm_snapshot = timeout_input(f"Are you sure you want to create a snapshot of the {instance_type} '{identifier}'? (yes/no): ",30)
        if confirm_snapshot.lower() != 'yes':
            logger.info(f"Snapshot creation for {instance_type} '{identifier}' skipped.")
            return None

        snapshot_name = f"{identifier}-snapshot-{datetime.now().strftime('%Y%m%d%H%M%S')}"
        if instance_type == 'RDS':
            logger.info(f"Creating snapshot '{snapshot_name}' for RDS instance '{identifier}'...")
            response = rds_client.create_db_snapshot(
                DBSnapshotIdentifier=snapshot_name,
                DBInstanceIdentifier=identifier
            )
            waiter = rds_client.get_waiter('db_snapshot_available')
            waiter.wait(DBSnapshotIdentifier=snapshot_name)
        elif instance_type == 'Aurora':
            logger.info(f"Creating snapshot '{snapshot_name}' for Aurora cluster '{identifier}'...")
            response = rds_client.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=snapshot_name,
                DBClusterIdentifier=identifier
            )
            waiter = rds_client.get_waiter('db_cluster_snapshot_available')
            waiter.wait(DBClusterSnapshotIdentifier=snapshot_name)
        else:
            logger.error("Unsupported instance type for snapshot creation.")
            return None

        logger.info(f"Snapshot '{snapshot_name}' created successfully.")
        return snapshot_name

    except ClientError as e:
        logger.error(f"Failed to create snapshot: {e}")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred: {e}")
        return None

def wait_for_bg_switchover(rds_client, identifier, bg_identifier, timeout=300, interval=30):
    starttime = time.monotonic()
    while time.monotonic() - starttime < timeout:
        switchover_status = check_blue_green_deployment_status(rds_client, identifier, bg_identifier)
        logger.info("Waiting for switchover to complete... current status: %s", switchover_status)

        if switchover_status == 'SWITCHOVER_COMPLETED':
            logger.info(f"Switchover completed in {time.monotonic() - starttime:.2f} seconds.")
            return switchover_status  # Exit early if successful

        # Sleep for the defined interval
        time.sleep(interval)

    logger.error("Timed out after waiting for switchover.")
    return switchover_status  # Return the last status on timeout

def timeout_input(prompt, timeout):
    """Function to get user input with a timeout."""
    user_input = [None]  # Using a list to store the input in the nested function.

    def get_input():
        try:
            user_input[0] = input(prompt)
        except Exception as e:
            logger.error(f"Error occurred: {e}")

    # Create and start a thread for input
    input_thread = threading.Thread(target=get_input, daemon=True)
    input_thread.start()

    # Wait for the specified timeout
    input_thread.join(timeout)

    if input_thread.is_alive():
        # Timeout occurred, return default value and clean up
        logger.info("\nTimeout reached. Defaulting to 'no'.")
        return 'no'
    else:
        return user_input[0]

def main():
    # Prompt user for the RDS or Aurora instance identifiers and target version
    args = parse_arguments()
    identifier = args.identifier
    target_version = args.target_version

    rds_client = initialize_aws_clients()
    
    db_instance, instance_type = validate_rds_or_aurora(rds_client, identifier)

    current_version = db_instance.get('EngineVersion', None)

    upgrade_needed = validate_versions(current_version, target_version)
        
    bg_identifier = get_blue_green_deployment_identifier(rds_client, identifier)
    
    switchover_status = None
    
    if upgrade_needed is False and switchover_status is None:
        print("No upgrade needed. Exiting.")
        sys.exit(0)

    if bg_identifier:
        switchover_status = check_blue_green_deployment_status(rds_client, identifier, bg_identifier)
    else:
        # replication_enabled = check_logical_replication(rds_client, db_instance, instance_type)
        if upgrade_needed and switchover_status is None and not check_pg_slots_and_extensions.fetch_and_check(identifier):
            create_snapshot(rds_client, identifier, instance_type)
            initiate_blue_green_upgrade(rds_client, identifier, target_version, instance_type)
    
    if switchover_status == 'AVAILABLE':
        switchover_status = switchover_blue_green_deployment(rds_client, bg_identifier)
            
    if switchover_status == 'SWITCHOVER_IN_PROGRESS':       
       switchover_status = wait_for_bg_switchover(rds_client, identifier, bg_identifier)
          
    if switchover_status == 'SWITCHOVER_COMPLETED':
            database_name = delete_blue_green_deployment(rds_client, bg_identifier, instance_type)
            if database_name is not None:
                delete_database_instance_or_cluster(rds_client, instance_type, database_name)

if __name__ == "__main__":
    main()