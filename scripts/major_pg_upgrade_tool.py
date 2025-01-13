from rds_upgrade_tool import *

def get_parameter_groups(identifier, rds_client, instance_type):
    """
    Retrieve the parameter group details for an RDS or Aurora cluster and its instances.

    :param identifier: The cluster or instance identifier.
    :param rds_client: Initialized RDS client.
    :return: Tuple with cluster parameter group (if applicable) and instance parameter group.
    """
    try:

        if instance_type == 'RDS':
            # Logic for RDS instances
            # Get all instances
            instances = rds_client.describe_db_instances()['DBInstances']

            # Find the instance matching the identifier
            for instance in instances:
                if instance['DBInstanceIdentifier'] == identifier:
                    # Retrieve the single instance parameter group
                    instance_parameter_group = instance['DBParameterGroups'][0]['DBParameterGroupName']
                    return None, instance_parameter_group  # No cluster parameter group for RDS

        elif instance_type == 'Aurora':
            # Logic for Aurora clusters
            # Get cluster details
            cluster_response = rds_client.describe_db_clusters(DBClusterIdentifier=identifier)
            print("Cluster Response:", cluster_response)
            cluster = cluster_response['DBClusters'][0]
            print("Cluster:", cluster)

            # Get cluster parameter group
            cluster_parameter_group = cluster['DBClusterParameterGroup']
            print("Cluster Parameter Group:", cluster_parameter_group)
            # Get instances in the cluster
            instances = rds_client.describe_db_instances(
                Filters=[{'Name': 'db-cluster-id', 'Values': [identifier]}]
            )['DBInstances']

            # Retrieve the single instance parameter group (all instances assumed to share the same group)
            instance_parameter_group = instances[0]['DBParameterGroups'][0]['DBParameterGroupName']

            return cluster_parameter_group, instance_parameter_group

        else:
            print("Unsupported instance type. Please use 'RDS' or 'Aurora'.")
            return None, None

    except ClientError as e:
        print(f"ClientError retrieving parameter groups: {e}")
        return None, None
    except Exception as e:
        print(f"Error retrieving parameter groups: {e}")
        return None, None

def create_cluster_parameter_group(rds_client, pgfamily, identifier):
    """Create a new Aurora DB cluster parameter group."""
    new_param_group_name = f"{identifier}-cluster-pg{pgfamily}"# Unique group name
    print("New Param Group Name:", new_param_group_name)
    description = f"{pgfamily} Parameter group for {identifier}"
    try:
        response = rds_client.create_db_cluster_parameter_group(
            DBClusterParameterGroupName=new_param_group_name,
            DBParameterGroupFamily=pgfamily,
            Description=description,
        )
        print(f"Cluster parameter group '{new_param_group_name}' created successfully.")
        return new_param_group_name
    except ClientError as e:
        print(f"Error creating cluster parameter group: {e}")
        raise

def create_instance_parameter_group(rds_client, pgfamily, identifier):
    """Create a new RDS DB instance parameter group."""
    new_param_group_name = f"{identifier}-instance-pg{pgfamily}"  # Unique group name
    description = f"{pgfamily} Parameter group for {identifier}"
    try:
        response = rds_client.create_db_parameter_group(
            DBParameterGroupName=new_param_group_name,
            DBParameterGroupFamily=pgfamily,
            Description=description,
        )
        print(f"Instance parameter group '{new_param_group_name}' created successfully.")
        return new_param_group_name
    except ClientError as e:
        print(f"Error creating instance parameter group: {e}")
        raise

def get_user_defined_cluster_parameters(rds_client, cluster_pg):
    """Retrieve user-defined parameters from a cluster parameter group."""
    try:
        paginator = rds_client.get_paginator('describe_db_cluster_parameters')
        user_params = []
        print("Cluster Parameter Group:", cluster_pg)
        for page in paginator.paginate(DBClusterParameterGroupName=cluster_pg):
            for param in page['Parameters']:
                if param.get('Source') == 'user':  # Include only user-defined parameters
                    user_params.append(param)

        print(f"Retrieved {len(user_params)} user-defined parameters from cluster group '{cluster_pg}'.")
        return user_params
    except ClientError as e:
        print(f"Error retrieving cluster parameters: {e}")
        raise

def get_user_defined_instance_parameters(rds_client, instance_pg):
    """Retrieve user-defined parameters from an instance parameter group."""
    try:
        paginator = rds_client.get_paginator('describe_db_parameters')
        user_params = []

        for page in paginator.paginate(DBParameterGroupName=instance_pg):
            for param in page['Parameters']:
                if param.get('Source') == 'user':  # Include only user-defined parameters
                    user_params.append(param)

        print(f"Retrieved {len(user_params)} user-defined parameters from instance group '{instance_pg}'.")
        return user_params
    except ClientError as e:
        print(f"Error retrieving instance parameters: {e}")
        raise

def apply_cluster_parameters(rds_client, cluster_pg, parameters):
    """Apply user-defined parameters to a cluster parameter group."""
    try:
        formatted_params = [
            {
                'ParameterName': param['ParameterName'],
                'ParameterValue': param['ParameterValue'],
                'ApplyMethod': 'pending-reboot'
            }
            for param in parameters
        ]

        if formatted_params:
            rds_client.modify_db_cluster_parameter_group(
                DBClusterParameterGroupName=cluster_pg,
                Parameters=formatted_params
            )
            print(f"Applied {len(formatted_params)} parameters to cluster group '{cluster_pg}'.")
        else:
            print(f"No user-defined parameters to apply to cluster group '{cluster_pg}'.")
    except ClientError as e:
        print(f"Error applying cluster parameters: {e}")
        raise

def apply_instance_parameters(rds_client, instance_pg, parameters):
    """Apply user-defined parameters to an instance parameter group."""
    try:
        formatted_params = [
            {
                'ParameterName': param['ParameterName'],
                'ParameterValue': param['ParameterValue'],
                'ApplyMethod': 'pending-reboot'
            }
            for param in parameters
        ]

        if formatted_params:
            rds_client.modify_db_parameter_group(
                DBParameterGroupName=instance_pg,
                Parameters=formatted_params
            )
            print(f"Applied {len(formatted_params)} parameters to instance group '{instance_pg}'.")
        else:
            print(f"No user-defined parameters to apply to instance group '{instance_pg}'.")
    except ClientError as e:
        print(f"Error applying instance parameters: {e}")
        raise

def handle_parameter_groups_upgrade(identifier, rds_client, current_version, target_version, instance_type):
    
    cluster_pg, instance_pg = get_parameter_groups(identifier, rds_client, instance_type)
    print("Cluster Parameter Group:", cluster_pg)
    
    is_major_upgrade = int(target_version.split('.')[0]) > int(current_version.split('.')[0])
 
    if is_major_upgrade:
        print(f"Major upgrade detected: {current_version} -> {target_version}")
        family = int(target_version.split('.')[0])
        if instance_type == 'Aurora':
            pgfamily = f"aurora-postgresql{family}"
            print("Cluster Parameter Group:", cluster_pg)
            new_param_group_name = create_cluster_parameter_group(rds_client, pgfamily, identifier)
            print("user params")
            user_params = get_user_defined_cluster_parameters(rds_client, cluster_pg)
            apply_cluster_parameters(rds_client, new_param_group_name, user_params)
            if instance_pg:
                print("Instance Parameter Group:", instance_pg)
                new_param_group_name = create_instance_parameter_group(rds_client, pgfamily, identifier)
                user_params = get_user_defined_instance_parameters(rds_client, instance_pg)
                apply_instance_parameters(rds_client, new_param_group_name, user_params)
        elif instance_type == 'RDS':
            print("Instance Parameter Group:", instance_pg)
            pgfamily = f"postgres{family}"
            new_param_group_name = create_instance_parameter_group(rds_client, pgfamily, identifier)
            user_params = get_user_defined_instance_parameters(rds_client, instance_pg)
            apply_instance_parameters(rds_client, new_param_group_name, user_params)
            
    else:
        print(f"Minor upgrade detected: {current_version} -> {target_version}")
        
# Example usage
if __name__ == "__main__":

    args = parse_arguments()
    identifier = args.identifier
    target_version = args.target_version
    rds_client = initialize_aws_clients()
    
    
    db_instance, instance_type = validate_rds_or_aurora(rds_client, identifier)

    current_version = db_instance.get('EngineVersion', None)
    
    handle_parameter_groups_upgrade(identifier, rds_client, current_version, target_version, instance_type)
    
