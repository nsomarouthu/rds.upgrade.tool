import sys
from rds_upgrade_tool import logger, initialize_aws_clients, validate_rds_or_aurora, parse_arguments
from botocore.exceptions import ClientError

PARAMETER_DOC_LINKS = {
    'max_replication_slots': 'https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/Appendix.PostgreSQL.CommonDBATasks.html#Appendix.PostgreSQL.CommonDBATasks.ReplicationSlots',
    'max_wal_senders': 'https://www.postgresql.org/docs/current/runtime-config-replication.html',
    'max_logical_replication_workers': 'https://www.postgresql.org/docs/current/runtime-config-replication.html',
    'max_worker_processes': 'https://www.postgresql.org/docs/current/runtime-config-resource.html',
    'rds.logical_replication': 'https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_PostgreSQL.Replication.html',
    'autovacuum_max_workers': 'https://www.postgresql.org/docs/current/runtime-config-autovacuum.html',
    'max_parallel_workers': 'https://www.postgresql.org/docs/current/runtime-config-resource.html',
    'synchronous_commit': 'https://www.postgresql.org/docs/current/runtime-config-wal.html#GUC-SYNCHRONOUS-COMMIT'
}


def fetch_parameters(describe_function, param_group_name, instance_type):
    """
    Fetch all parameters from the given parameter group with pagination.
    """
    parameters = []
    marker = ''
    while True:
        if instance_type == 'RDS':
            response = describe_function(DBParameterGroupName=param_group_name, Marker=marker)
        elif instance_type == 'Aurora':
            response = describe_function(DBClusterParameterGroupName=param_group_name, Marker=marker)
        
        parameters.extend(response.get('Parameters', []))
        marker = response.get('Marker', '')
        if not marker:
            break
    return parameters


def display_parameters(parameters):
    """
    Display the specified parameters and their current values.
    """
    logger.info("Current Parameter Values:")
    modifiable_parameters = []

    for param in parameters:
        if param['ParameterName'] in PARAMETER_DOC_LINKS:
            value = param.get('ParameterValue', 'Not Set')
            modifiable_parameters.append(param)
            logger.info(f"Parameter: {param['ParameterName']}, Value: {value}")

    return modifiable_parameters


def modify_parameters(rds_client, param_group_name, instance_type, parameters):
    """
    Modify parameters in the given parameter group.
    """
    if instance_type == 'RDS':
        rds_client.modify_db_parameter_group(
            DBParameterGroupName=param_group_name,
            Parameters=parameters
        )
    elif instance_type == 'Aurora':
        rds_client.modify_db_cluster_parameter_group(
            DBClusterParameterGroupName=param_group_name,
            Parameters=parameters
        )
    logger.info("Parameters modified. Changes pending reboot.")


def print_user_defined_parameters(rds_client, parameter_group_name, instance_type):
    """Print all parameters with Source: user, handling pagination."""
    try:
        paginator_method = (
            rds_client.get_paginator('describe_db_cluster_parameters')
            if instance_type.lower() == 'aurora'
            else rds_client.get_paginator('describe_db_parameters')
        )
        
        paginator_key = (
            "DBClusterParameterGroupName"
            if instance_type.lower() == 'aurora'
            else "DBParameterGroupName"
        )
        
        user_params = []
        paginator = paginator_method.paginate(**{paginator_key: parameter_group_name})
        for page in paginator:
            for param in page['Parameters']:
                if param.get('Source') == 'user':  # Check if 'Source' is 'user'
                    user_params.append(param)

        if user_params:
            print(f"\nParameters with Source: user in {parameter_group_name}:")
            for param in user_params:
                print(f"{param['ParameterName']}: {param['ParameterValue']}")
        else:
            print(f"\nNo parameters with Source: user found in {parameter_group_name}")
    except rds_client.exceptions.DBParameterGroupNotFoundFault:
        sys.exit(f"Error: The parameter group '{parameter_group_name}' does not exist.")
    except Exception as e:
        sys.exit(f"Error retrieving parameters: {e}")


def check_and_update_parameters(rds_client, db_instance, instance_type):
    """
    Check and allow the user to modify specific parameters if necessary.
    """
    try:
        # Determine the parameter group and describe function
        if 'DBParameterGroups' in db_instance:
            param_group_name = db_instance['DBParameterGroups'][0]['DBParameterGroupName']
        elif 'DBClusterParameterGroup' in db_instance:
            param_group_name = db_instance['DBClusterParameterGroup']
        else:
            logger.error(f"No parameter group found in db_instance: {db_instance}")
            return False

        describe_function = (
            rds_client.describe_db_parameters 
            if instance_type == 'RDS' 
            else rds_client.describe_db_cluster_parameters
        )

        # Fetch and display parameters
        parameters = fetch_parameters(describe_function, param_group_name, instance_type)
        modifiable_parameters = display_parameters(parameters)

        # --- NEW: Print user-defined parameters (optional step) ---
        print_user_defined_parameters(rds_client, param_group_name, instance_type)

        # Ask the user if they want to modify any parameter
        changes = []
        for param in modifiable_parameters:
            name = param['ParameterName']
            current_value = param.get('ParameterValue', 'Not Set')
            user_input = input(
                f"\nDo you want to change '{name}'? Current value: {current_value}\n"
                f"Refer to Documentation: {PARAMETER_DOC_LINKS[param['ParameterName']]}\n"
                "Enter new value or press Enter to skip: "
            )
            if user_input:
                changes.append({
                    'ParameterName': name,
                    'ParameterValue': user_input,
                    'ApplyMethod': 'pending-reboot'
                })

        # Apply changes if any
        if changes:
            modify_parameters(rds_client, param_group_name, instance_type, changes)
            logger.info("Changes have been applied. Please reboot the instance to take effect.")
        else:
            logger.info("No changes made.")

        return True

    except ClientError as e:
        logger.error(f"Client error occurred: {e}")
        return False
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False


if __name__ == "__main__":
    # Initialize AWS clients and validate the instance
    rds_client = initialize_aws_clients()
    args = parse_arguments()
    identifier = args.identifier
    target_version = args.target_version if args.target_version else 'default_version'
    
    db_instance, instance_type = validate_rds_or_aurora(rds_client, identifier)
    current_version = db_instance.get('EngineVersion', None)

    # Run the check and update function
    check_and_update_parameters(rds_client, db_instance, instance_type)
