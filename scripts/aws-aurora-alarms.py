import boto3

# Assign parsed arguments to variables
source_instance = "nstar-dnsconf-20230417-prd-1-prod"
target_writer_instance = "rds-aurora-postgres-rds-aurora-pg-nstar-dns-c-rds2-fc0cqxs0jy4e"  # Target writer instance
target_reader_instance = "rds-aurora-postgres-rds-aurora-pg-nstar-dns-c-rds1-uc19cl13cnhe"  # Target reader instance
target_alarm_name_identifier = "nstar-dns-conf-20240904-prd-1-prod-a    a"  # Aurora reader instance

# Initialize CloudWatch client
cloudwatch = boto3.client('cloudwatch', region_name='us-east-1')

# Initialize list to store all alarms
all_alarms = []
next_token = None

# Fetch alarms with pagination
while True:
    if next_token:
        alarms = cloudwatch.describe_alarms(NextToken=next_token)
    else:
        alarms = cloudwatch.describe_alarms()

    all_alarms.extend(alarms['MetricAlarms'])  # Add fetched alarms to the list

    # If there are more alarms, fetch the next page
    next_token = alarms.get('NextToken')
    if not next_token:
        break  # Exit the loop when all alarms are fetched

# Check if any alarms exist
if not all_alarms:
    print("No alarms found.")
else:
    print(f"Total alarms found: {len(all_alarms)}")

# Loop through alarms to see if any match the source instance
for alarm in all_alarms:
    print(f"Found alarm: {alarm['AlarmName']}")  # Print each alarm name for debugging

    # Check if the alarm is for the source instance
    if source_instance in alarm['AlarmName']:
        print(f"Processing alarm: {alarm['AlarmName']}")

        # Modify alarm details for both the writer and reader instances
        new_alarm_name_writer = alarm['AlarmName'].replace(source_instance, f"{target_alarm_name_identifier}-writer")
        new_alarm_name_reader = alarm['AlarmName'].replace(source_instance, f"{target_alarm_name_identifier}-reader")
        
        print(f"New alarm name for writer will be: {new_alarm_name_writer}")
        print(f"New alarm name for reader will be: {new_alarm_name_reader}")
        
        # Modify the dimensions for both writer and reader instances
        new_dimensions_writer = []
        new_dimensions_reader = []
        
        # Check for DBClusterIdentifier (Aurora Cluster) or DBInstanceIdentifier (Instance)
        if 'DBClusterIdentifier' in [dim['Name'] for dim in alarm['Dimensions']]:
            # Aurora Cluster (both writer and reader instances)
            new_dimensions_writer.append({'Name': 'DBClusterIdentifier', 'Value': target_writer_instance})
            new_dimensions_reader.append({'Name': 'DBClusterIdentifier', 'Value': target_reader_instance})
        else:
            # RDS Instance (both writer and reader instances)
            new_dimensions_writer.append({'Name': 'DBInstanceIdentifier', 'Value': target_writer_instance})
            new_dimensions_reader.append({'Name': 'DBInstanceIdentifier', 'Value': target_reader_instance})

        # Update alarm name and dimensions for the writer and reader instances
        alarm['AlarmName'] = new_alarm_name_writer
        alarm['Dimensions'] = new_dimensions_writer

        # Remove keys not needed in put_metric_alarm
        keys_to_remove = [
            'AlarmArn', 'StateValue', 'StateReason', 'StateReasonData',
            'StateUpdatedTimestamp', 'StateTransitionedTimestamp', 'AlarmConfigurationUpdatedTimestamp'
        ]
        for key in keys_to_remove:
            alarm.pop(key, None)

        # Print the final alarm configuration for writer
        print(f"Final alarm configuration for writer: {alarm}")

        # Create new alarm for the writer instance
        try:
            cloudwatch.put_metric_alarm(**alarm)
            print(f"Created alarm {new_alarm_name_writer} for {target_writer_instance}")
        except Exception as e:
            print(f"Failed to create alarm {new_alarm_name_writer} for {target_writer_instance}: {str(e)}")

        # Now create the alarm for the reader instance
        alarm['AlarmName'] = new_alarm_name_reader
        alarm['Dimensions'] = new_dimensions_reader

        # Print the final alarm configuration for reader
        print(f"Final alarm configuration for reader: {alarm}")

        # Create new alarm for the reader instance
        try:
            cloudwatch.put_metric_alarm(**alarm)
            print(f"Created alarm {new_alarm_name_reader} for {target_reader_instance}")
        except Exception as e:
            print(f"Failed to create alarm {new_alarm_name_reader} for {target_reader_instance}: {str(e)}")
