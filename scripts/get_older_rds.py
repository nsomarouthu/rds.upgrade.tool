from rds_upgrade_tool import *


def parse_engine_version(version):
    """
    Converts an engine version string to a tuple of integers for comparison.
    Non-numeric versions return a high value tuple (e.g., (float('inf'),)).
    """
    try:
        return tuple(int(part) for part in version.split('.') if part.isdigit())
    except ValueError:
        return (float('inf'),)


def version_less_than(version1, version2):
    """
    Compares two version tuples to check if version1 < version2.
    Missing parts in a version are considered as 0.
    """
    max_length = max(len(version1), len(version2))
    version1 += (0,) * (max_length - len(version1))
    version2 += (0,) * (max_length - len(version2))
    return version1 < version2


def filter_and_collect_rds_instances(rds_client, max_engine_version_tuple):
    """
    Retrieves and filters RDS instances based on the max engine version.
    """
    instances = []
    instance_count = 0

    for db_instance in rds_client.describe_db_instances()['DBInstances']:
        if 'aurora' in db_instance['Engine'].lower():
            continue  # Skip Aurora instances

        engine_version = parse_engine_version(db_instance['EngineVersion'])
        if max_engine_version_tuple is None or version_less_than(engine_version, max_engine_version_tuple):
            instances.append({
                'DBInstanceIdentifier': db_instance['DBInstanceIdentifier'],
                'EngineVersion': db_instance['EngineVersion']
            })
            instance_count += 1

    return instances, instance_count


def filter_and_collect_rds_clusters(rds_client, max_engine_version_tuple):
    """
    Retrieves and filters RDS clusters based on the max engine version.
    """
    clusters = []
    cluster_count = 0

    for db_cluster in rds_client.describe_db_clusters()['DBClusters']:
        engine_version = parse_engine_version(db_cluster['EngineVersion'])
        if max_engine_version_tuple is None or version_less_than(engine_version, max_engine_version_tuple):
            clusters.append({
                'DBClusterIdentifier': db_cluster['DBClusterIdentifier'],
                'EngineVersion': db_cluster['EngineVersion']
            })
            cluster_count += 1

    return clusters, cluster_count


def main():
    """
    Main script logic to filter and display RDS instances and clusters.
    """
    # Parse command-line argument for max_engine_version
    if len(sys.argv) != 2:
        print("No max_engine_version argument provided. Printing all instances and clusters.")
        max_engine_version_tuple = None
    else:
        try:
            max_engine_version = sys.argv[1]
            max_engine_version_tuple = parse_engine_version(max_engine_version)
        except ValueError:
            sys.exit("Error: The provided max_engine_version argument must be a valid version string.")

    # Initialize AWS RDS client
    rds_client = initialize_aws_clients()

    # Collect and filter RDS instances and clusters
    rds_instances, instance_count = filter_and_collect_rds_instances(rds_client, max_engine_version_tuple)
    rds_clusters, cluster_count = filter_and_collect_rds_clusters(rds_client, max_engine_version_tuple)

    # Sort instances and clusters by engine version
    rds_instances.sort(key=lambda x: parse_engine_version(x['EngineVersion']))
    rds_clusters.sort(key=lambda x: parse_engine_version(x['EngineVersion']))

    # Display results
    print("RDS Instances:")
    for instance in rds_instances:
        print(f" EngineVersion: {instance['EngineVersion']} | Instance ID: {instance['DBInstanceIdentifier']}")

    print("\nRDS Clusters:")
    for cluster in rds_clusters:
        print(f" EngineVersion: {cluster['EngineVersion']} | Cluster ID: {cluster['DBClusterIdentifier']}")

    # Summary
    print(f"\nTotal RDS Instances: {instance_count}")
    print(f"Total RDS Clusters: {cluster_count}")


if __name__ == "__main__":
    main()
