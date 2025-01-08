# RDS PostgreSQL Management & Upgrade Toolkit

This repository contains **four Python scripts** designed to help you **discover**, **manage**, and **upgrade** Amazon RDS/Aurora PostgreSQL instances. Below is an overview of each script, its purpose, and usage instructions. These tools collectively provide a streamlined workflow for PostgreSQL version analysis, parameter management, replication slot checks, and orchestrating a Blue/Green deployment upgrade.

---

## Table of Contents

1. [Overview of Scripts](#overview-of-scripts)  
   1.1 [1. get_older_rds.py](#1-get_older_rdspy)  
   1.2 [2. rds_parameter_group_tool.py](#2-rds_parameter_group_toolpy)  
   1.3 [3. check_pg_slots_and_extensions.py](#3-check_pg_slots_and_extensionspy)  
   1.4 [4. rds_upgrade_tool.py](#4-rds_upgrade_toolpy)  

2. [Setup and Prerequisites](#setup-and-prerequisites)  
   2.1 [Python Version](#python-version)  
   2.2 [AWS Credentials](#aws-credentials)  
   2.3 [Required Python Libraries](#required-python-libraries)  

3. [Usage](#usage)  
   3.1 [Running Each Script Individually](#running-each-script-individually)  
   3.2 [Recommended Workflow](#recommended-workflow)  

4. [Environment Variables](#environment-variables)  


---

## Overview of Scripts

### 1. get_older_rds.py

**Purpose**  
- Connects to your AWS environment and retrieves **RDS** (non-Aurora) instances and **Aurora** clusters.
- Optionally filters them by a **maximum engine version** you specify (e.g., list only those older than version 13).
- Sorts and prints their engine versions and identifiers, along with a summary count.

**Key Functions**  
- **`parse_engine_version(version)`**: Converts a version string into a tuple (e.g., `"12.7"` -> `(12, 7)`) for comparison.  
- **`filter_and_collect_rds_instances(...)`**: Returns RDS instances matching the filter.  
- **`filter_and_collect_rds_clusters(...)`**: Returns Aurora clusters matching the filter.

**Usage**  
```bash
python get_older_rds.py [MAX_ENGINE_VERSION]

### 2. rds_parameter_group_tool.py

**Purpose**  
- **Retrieves**, **displays**, and **optionally modifies** a subset of **PostgreSQL** parameters on an RDS or Aurora parameter group.
- Prints user-defined parameters (i.e., `Source=user`) to identify which have been customized.
- Prompts for new parameter values, then applies them **pending a reboot**.

**Key Functions**  
- **`fetch_parameters(...)`**: Fetches parameters (paginated) using either `describe_db_parameters` or `describe_db_cluster_parameters`.  
- **`display_parameters(...)`**: Logs only PostgreSQL parameters listed in a dictionary (`PARAMETER_DOC_LINKS`).  
- **`modify_parameters(...)`**: Applies updated parameter values with `ApplyMethod='pending-reboot'`.  
- **`print_user_defined_parameters(...)`**: Lists parameters where `Source=user`.  
- **`check_and_update_parameters(...)`**: Main interactive flow, prompting the user to modify parameters if needed.

**Usage**  
```bash
python rds_parameter_group_tool.py --identifier <DB_IDENTIFIER> [--target_version <ENGINE_VERSION>]

### 3. check_pg_slots_and_extensions.py

**Purpose**  
- Connects to a **PostgreSQL** database using credentials from **AWS Secrets Manager**.
- Checks for **active replication slots** and **flagged extensions** (e.g., `pg_partman`, `pg_cron`, `pglogical`, `pgactive`, `pgaudit`).
- Exits with **code 1** if any active replication slots or flagged extensions are found; otherwise, exits with **0**.

**Key Functions**  
- **`get_secret(instance_name, region_name="us-east-1")`**: Fetches the secret named `athena/rds/<instance_name>/root` from Secrets Manager.  
- **`check_active_replication_slots(...)`**: Queries `pg_replication_slots` for any `active = true` slots.  
- **`check_extensions(...)`**: Queries `pg_extension` and flags if certain extensions are installed.  
- **`fetch_and_check(instance_name, region_name="us-east-1")`**: High-level function that retrieves secrets and performs both checks.

**Usage**  
```bash
python check_pg_slots_and_extensions.py --instance <RDS_INSTANCE_NAME> [--region <AWS_REGION>]

### 4. rds_upgrade_tool.py

**Purpose**  
- Automates a **Blue/Green** deployment upgrade for a PostgreSQL **RDS or Aurora** instance.
- Validates whether an upgrade is required (comparing current vs. target engine version).
- If needed, it creates (or finds) a **Blue/Green deployment**, initiates the upgrade, performs a **switchover**, and optionally **deletes** the old environment.

**Key Functions**  
- **`parse_arguments()`**: Retrieves `--identifier` and `--target-version` from the command line.  
- **`initialize_aws_clients()`**: Validates AWS environment variables and sets up an RDS client.  
- **`validate_rds_or_aurora(rds_client, identifier)`**: Checks if the instance is Aurora or RDS.  
- **`validate_versions(current_version, target_version)`**: Ensures the target version is newer than the current version (no downgrades).  
- **`initiate_blue_green_upgrade(rds_client, identifier, db_engine_version, instance_type)`**: Creates a Blue/Green deployment if none exists.  
- **`switchover_blue_green_deployment(rds_client, bg_identifier)`**: Initiates a switchover to the upgraded environment.  
- **`wait_for_bg_switchover(...)`**: Polls for the switchover to finish.  
- **`delete_blue_green_deployment(...)`** and **`delete_database_instance_or_cluster(...)`**: Cleans up old resources after switchover.  
- Integrates with **`check_pg_slots_and_extensions.fetch_and_check(identifier)`** to ensure no active replication slots or flagged extensions before upgrading.

**Usage**  
```bash
python rds_upgrade_tool.py -i <DB_IDENTIFIER> -t <TARGET_VERSION>
