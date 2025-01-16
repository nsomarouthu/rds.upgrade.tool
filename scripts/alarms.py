from rds_upgrade_tool import *

def fetch_all_cloudwatch_alarms(cloudwatch):
    """
    Fetches all CloudWatch alarms in the specified AWS region.

    Parameters:
        region_name (str): The AWS region to query. Defaults to 'us-east-1'.

    Returns:
        list: A list of CloudWatch alarm dictionaries.
    """
    try:
        
        # Initialize list to store all alarms
        all_alarms = []
        next_token = None
        
        # Fetch alarms with pagination
        while True:
            if next_token:
                response = cloudwatch.describe_alarms(NextToken=next_token)
            else:
                response = cloudwatch.describe_alarms()
        
            # Add fetched alarms to the list
            alarms = response.get('MetricAlarms', [])
            all_alarms.extend(alarms)
        
            # Check if there are more alarms to fetch
            next_token = response.get('NextToken')
            if not next_token:
                break  # Exit the loop when all alarms are fetched
        
        # Check if any alarms exist and print the result
        if not all_alarms:
            print("No alarms found.")
        else:
            print(f"Total alarms found: {len(all_alarms)}")
        
        return all_alarms

    except ClientError as error:
        print(f"An error occurred: {error}")
        return []

def create_alarms(all_alarms, source_instance, target_instance, cloudwatch):
    """
    Processes CloudWatch alarms by finding alarms associated with the source_instance,
    modifying them for the target_instance, and creating new alarms.

    Parameters:
    - all_alarms (list): List of alarms retrieved from CloudWatch.
    - source_instance (str): The source instance identifier to search for in alarm names.
    - target_instance (str): The target instance identifier to set in the new alarm names and dimensions.
    - cloudwatch (boto3.client): Boto3 CloudWatch client.
    """
    # Derive target_alarm_name_identifier from target_instance
    target_alarm_name_identifier = f"{target_instance}-alarm"

    for alarm in all_alarms:
        alarm_name = alarm.get('AlarmName', 'Unnamed Alarm')
        print(f"Found alarm: {alarm_name}")  # Print each alarm name for debugging

        # Check if the alarm is for the source instance
        if source_instance in alarm_name:
            print(f"Processing alarm: {alarm_name}")

            # Modify alarm details for the writer instance
            new_alarm_name_writer = alarm_name.replace(source_instance, f"{target_alarm_name_identifier}-writer")
            print(f"New alarm name for writer will be: {new_alarm_name_writer}")

            # Modify the dimensions for the writer instance
            new_dimensions_writer = []
            dimension_names = [dim['Name'] for dim in alarm.get('Dimensions', [])]
            
            if 'DBClusterIdentifier' in dimension_names:
                # Aurora Cluster (both writer and reader instances)
                new_dimensions_writer.append({'Name': 'DBClusterIdentifier', 'Value': target_instance})
            else:
                # RDS Instance (both writer and reader instances)
                new_dimensions_writer.append({'Name': 'DBInstanceIdentifier', 'Value': target_instance})

            # Create a copy of the alarm to modify for the writer
            new_alarm_writer = alarm.copy()
            new_alarm_writer['AlarmName'] = new_alarm_name_writer
            new_alarm_writer['Dimensions'] = new_dimensions_writer

            # Remove keys not needed in put_metric_alarm
            keys_to_remove = [
                'AlarmArn', 'StateValue', 'StateReason', 'StateReasonData',
                'StateUpdatedTimestamp', 'StateTransitionedTimestamp', 'AlarmConfigurationUpdatedTimestamp'
            ]
            for key in keys_to_remove:
                new_alarm_writer.pop(key, None)

            # Print the final alarm configuration for writer
            print(f"Final alarm configuration for writer: {new_alarm_writer}")

            # Create new alarm for the writer instance
            try:
                cloudwatch.put_metric_alarm(**new_alarm_writer)
                print(f"Created alarm {new_alarm_name_writer} for {target_instance}")
            except ClientError as e:
                print(f"Failed to create alarm {new_alarm_name_writer} for {target_instance}: {e.response['Error']['Message']}")

def print_db_instance_details(rds_client, instance_type, identifier):
    """
    Prints the identifier names and endpoint addresses of RDS or Aurora instances.

    Args:
        rds_client: Boto3 RDS client.
        instance_type (str): Type of the instance ('RDS' or 'Aurora').
        identifier (str): The identifier value to filter instances.
    """
    # Define mappings for filter names and identifier fields based on instance type
    filter_mapping = {
        'RDS': {
            'filter_name': 'db-instance-id',
            'identifier_field': 'DBInstanceIdentifier'
        },
        'Aurora': {
            'filter_name': 'db-cluster-id',
            'identifier_field': 'DBInstanceIdentifier'
        }
    }
    
    # Retrieve the appropriate filter details based on the instance type
    filter_details = filter_mapping.get(instance_type)
    
    if not filter_details:
        print(f"Unsupported instance type: {instance_type}")
        return
    
    filter_name = filter_details['filter_name']
    identifier_field = filter_details['identifier_field']
    
    try:
        # Describe DB instances using the selected filter
        response = rds_client.describe_db_instances(
            Filters=[{'Name': filter_name, 'Values': [identifier]}]
        )
        
        # Extract DBInstances from the response
        instances = response.get('DBInstances', [])
        
        if not instances:
            print(f"No instances found for identifier '{identifier}' and type '{instance_type}'.")
            return
        
        # Iterate through each instance and print identifier and endpoint address
        for instance in instances:
            # Retrieve the identifier name based on instance type
            id_name = instance.get(identifier_field, 'N/A')
            
            # Retrieve the endpoint address
            endpoint = instance.get('Endpoint', {})
            # address = endpoint.get('Address', 'No endpoint address found')
            
            print(f"Identifier: {id_name}, Endpoint Address: {address}")
        
        return id_name
    
    except Exception as e:
        print(f"An error occurred while describing DB instances: {e}")


# Example usage
if __name__ == "__main__":

    args = parse_arguments()
    source_instance = args.identifier
    target_instance = args.target_version
    rds_client = initialize_aws_clients()
    # Initialize CloudWatch client
    cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')
    
    db_instance, source_instance_type = validate_rds_or_aurora(rds_client, source_instance)
    db_instance, target_instance_type = validate_rds_or_aurora(rds_client, target_instance)
    
    if source_instance_type == 'Aurora':
        source_instance = print_db_instance_details(rds_client, source_instance_type, source_instance)
        if len(source_instance) > 1:
            source_instance = source_instance[0]
    
    if target_instance_type == 'Aurora':
        target_instance = print_db_instance_details(rds_client, target_instance_type, target_instance)
        
    all_alarms = fetch_all_cloudwatch_alarms(cloudwatch)
    for instance in target_instance:
            create_alarms(all_alarms, source_instance, instance, cloudwatch)
            print(f"Alarms created for {instance}")