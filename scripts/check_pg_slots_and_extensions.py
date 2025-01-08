import psycopg2
from psycopg2 import sql
import boto3
import json
import argparse

def get_secret(instance_name, region_name="us-east-1"):
    """
    Fetch secret from AWS Secrets Manager using the instance name.
    """
    try:
        secret_name = f"athena/rds/{instance_name}/root"
        session = boto3.session.Session()
        client = session.client(service_name='secretsmanager', region_name=region_name)
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)

        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            return json.loads(secret)
        else:
            raise ValueError(f"SecretString not found in {secret_name}")

    except Exception as e:
        raise RuntimeError(f"Error fetching secret {secret_name}: {e}")

def check_active_replication_slots(host, port, database, user, password):
    """
    Check for active replication slots in the PostgreSQL database.
    """
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=30 # 30 seconds timeout
        )
        cursor = connection.cursor()
        query = sql.SQL("""
            SELECT slot_name, active 
            FROM pg_replication_slots
            WHERE active = true;
        """)
        cursor.execute(query)
        active_slots = cursor.fetchall()

        if active_slots:
            print("Active replication slots found:")
            for slot in active_slots:
                print(f"Slot Name: {slot[0]}, Active: {slot[1]}")
            return True
        else:
            print("No active replication slots found.")
            return False

    except psycopg2.Error as e:
        raise RuntimeError(f"Error while checking replication slots: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def check_extensions(host, port, database, user, password):
    """
    Check and flag specific PostgreSQL extensions if present.
    """
    try:
        connection = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=30
        )
        cursor = connection.cursor()

        cursor.execute("SELECT extname FROM pg_extension;")
        installed_extensions = [row[0] for row in cursor.fetchall()]

        flagged_extensions = {
            "pg_partman": "Should be disabled in blue environments.",
            "pg_cron": "Should remain disabled in green environments.",
            "pglogical": "Should be disabled in blue environments.",
            "pgactive": "Should be disabled in blue environments.",
            "pgaudit": "Must remain in shared_preload_libraries."
        }

        for extension in installed_extensions:
            if extension in flagged_extensions:
                print(f"Flagged extension found: {extension}. Reason: {flagged_extensions[extension]}")
                return True

        return False

    except psycopg2.Error as e:
        raise RuntimeError(f"Error while checking extensions: {e}")

    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def fetch_and_check(instance_name, region_name="us-east-1"):
    try:
        rds_secret = get_secret(instance_name, region_name)

        host = rds_secret.get("host")
        port = rds_secret.get("port", 5432)
        database = rds_secret.get("databaseName", "postgres")
        user = rds_secret.get("username", "root")
        password = rds_secret.get("password")

        if host and password:
            replication_slots_flag = check_active_replication_slots(host, port, database, user, password)
            extensions_flag = check_extensions(host, port, database, user, password)

            return replication_slots_flag or extensions_flag
        else:
            raise ValueError("Missing host or password in the secret.")

    except Exception as e:
        raise RuntimeError(f"Failed to retrieve secret or check extensions: {e}")

# Example standalone script usage
def main():
    parser = argparse.ArgumentParser(description="Check PostgreSQL replication slots and extensions in a PostgreSQL database.")
    parser.add_argument("--instance", required=True, help="RDS instance identifier")
    parser.add_argument("--region", default="us-east-1", help="AWS region for Secrets Manager")
    args = parser.parse_args()

    print(f"Using instance name: {args.instance}")

    try:
        result = fetch_and_check(args.instance, args.region)
        if result:
            exit(1)
        else:
            print("Result: Zero")
            exit(0)
    except Exception as e:
        print(f"Error: {e}")
        exit(1)

if __name__ == "__main__":
    main()
