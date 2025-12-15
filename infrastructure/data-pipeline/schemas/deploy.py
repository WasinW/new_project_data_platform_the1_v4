#!/usr/bin/env python3
"""
BigQuery Table Deployer
=======================
Deploy tables from JSON definitions with smart change detection.

Usage:
    python deploy.py <PROJECT_ID> <DATASET_ID> <ENV> [--force]

Examples:
    python deploy.py the1-insight-stg insight stg
    python deploy.py the1-insight-prod insight prod
     --force
"""

import json
import subprocess
import sys
import os
from datetime import datetime
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum


class ChangeType(Enum):
    NO_CHANGE = "no_change"
    ADDITIVE = "additive"
    BREAKING = "breaking"


@dataclass
class TableChange:
    change_type: ChangeType
    added_columns: List[str]
    modified_columns: List[str]
    removed_columns: List[str]
    other_changes: List[str]


class TableDeployer:
    def __init__(self, project_id: str, dataset_id: str, env: str, region: str = "asia-southeast1"):
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.env = env
        self.region = region
        self.storage_bucket = f"the1-insight-{env}-data-pipeline-data-staging"

    def run_bq(self, args: List[str], check: bool = True) -> subprocess.CompletedProcess:
        # Always include project_id for bq commands
        cmd = ["bq", f"--project_id={self.project_id}"] + args
        # bq query
        #  --use_legacy_sql=false
        #  --nouse_cache
        #  --destination_table=your_project_id:your_dataset.your_table_name
        #  --create_disposition=CREATE_IF_NEEDED
        #  --write_disposition=WRITE_EMPTY
        #  --batch "CREATE TABLE `your_project_id.your_dataset.your_table_name` ( column1 STRING, column2 INT64, column3 TIMESTAMP );"
        result = subprocess.run(cmd, capture_output=True, text=True)
        if check and result.returncode != 0:
            print(f"  [ERROR] bq command failed: {' '.join(cmd)}")
            print(f"  stderr: {result.stderr}")
        return result

    def table_exists(self, table_id: str) -> bool:
        result = self.run_bq([
            "show", f"{self.project_id}:{self.dataset_id}.{table_id}"
        ], check=False)
        return result.returncode == 0

    def get_current_metadata(self, table_id: str) -> Optional[Dict]:
        result = self.run_bq([
            "show", "--format=json", f"{self.project_id}:{self.dataset_id}.{table_id}"
        ], check=False)

        if result.returncode != 0:
            return None

        try:
            return json.loads(result.stdout)
        except json.JSONDecodeError:
            return None

    def compare_schemas(self, current: Dict, expected: Dict) -> TableChange:
        added = []
        modified = []
        removed = []
        other_changes = []

        current_schema = {col["name"]: col for col in current.get("schema", {}).get("fields", [])}
        expected_schema = {col["name"]: col for col in expected.get("schema", [])}

        for name in expected_schema:
            if name not in current_schema:
                added.append(name)

        for name in current_schema:
            if name not in expected_schema:
                removed.append(name)

        for name in expected_schema:
            if name in current_schema:
                exp_type = expected_schema[name].get("type", "").upper()
                cur_type = current_schema[name].get("type", "").upper()
                if exp_type != cur_type:
                    modified.append(f"{name}: {cur_type} -> {exp_type}")

        current_partition = current.get("timePartitioning", {})
        expected_partition = expected.get("partitioning", {})

        if expected_partition:
            if current_partition.get("field") != expected_partition.get("field"):
                other_changes.append(f"Partition field: {current_partition.get('field')} -> {expected_partition.get('field')}")
            if current_partition.get("type") != expected_partition.get("type"):
                other_changes.append(f"Partition type changed")

        current_clustering = current.get("clustering", {}).get("fields", [])
        expected_clustering = expected.get("clustering", [])

        if current_clustering != expected_clustering:
            other_changes.append(f"Clustering: {current_clustering} -> {expected_clustering}")

        if modified or removed or other_changes:
            change_type = ChangeType.BREAKING
        elif added:
            change_type = ChangeType.ADDITIVE
        else:
            change_type = ChangeType.NO_CHANGE

        return TableChange(
            change_type=change_type,
            added_columns=added,
            modified_columns=modified,
            removed_columns=removed,
            other_changes=other_changes
        )

    def generate_create_sql(self, definition: Dict) -> str:
        table_id = definition["table_id"]
        table_type = definition.get("table_type", "native")
        schema = definition.get("schema", [])

        columns = []
        for col in schema:
            col_def = f"  {col['name']} {col['type']}"
            if col.get("mode") == "REQUIRED":
                col_def += " NOT NULL"
            if col.get("description"):
                col_def += f" OPTIONS(description=\"{col['description']}\")"
            columns.append(col_def)

        columns_sql = ",\n".join(columns)

        if table_type == "iceberg":
            biglake = definition.get("biglake_config", {})
            connection_id = f"{self.project_id}.{self.region}.{biglake.get('connection_id', '')}"
            storage_uri = f"gs://{self.storage_bucket}/{biglake.get('storage_uri_suffix', '')}"

            sql = f"""CREATE TABLE IF NOT EXISTS `{self.project_id}.{self.dataset_id}.{table_id}` (
{columns_sql}
)
CLUSTER BY {', '.join(definition.get('clustering', []))}
WITH CONNECTION `{connection_id}`
OPTIONS(
  file_format = '{biglake.get('file_format', 'PARQUET')}',
  table_format = '{biglake.get('table_format', 'ICEBERG')}',
  storage_uri = '{storage_uri}'
)"""
        else:
            sql = f"""CREATE TABLE IF NOT EXISTS `{self.project_id}.{self.dataset_id}.{table_id}` (
{columns_sql}"""

            if definition.get("primary_key"):
                pk_cols = ", ".join(definition["primary_key"])
                sql += f",\n  PRIMARY KEY ({pk_cols}) NOT ENFORCED"

            sql += "\n)"

            partitioning = definition.get("partitioning", {})
            if partitioning:
                # sql += f"\nPARTITION BY {partitioning.get('type', 'DAY')}({partitioning.get('field')})"
                field = partitioning.get('field')
                part_type = partitioning.get('type', 'DAY').upper()
                # BigQuery uses DATE() for TIMESTAMP columns with daily partitioning
                # or DATE_TRUNC for other granularities
                if part_type == 'DAY':
                    sql += f"\nPARTITION BY DATE({field})"
                else:
                    sql += f"\nPARTITION BY DATE_TRUNC({field}, {part_type})"

            clustering = definition.get("clustering", [])
            if clustering:
                sql += f"\nCLUSTER BY {', '.join(clustering)}"

        return sql

    def generate_alter_sql(self, table_id: str, columns: List[Dict]) -> str:
        alterations = []
        for col in columns:
            col_def = f"ADD COLUMN {col['name']} {col['type']}"
            if col.get("description"):
                col_def += f" OPTIONS(description=\"{col['description']}\")"
            alterations.append(col_def)

        return f"ALTER TABLE `{self.project_id}.{self.dataset_id}.{table_id}` {', '.join(alterations)}"

    def backup_table(self, table_id: str) -> str:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{table_id}_backup_{timestamp}"

        print(f"  [BACKUP] Creating backup: {backup_name}")
        result = self.run_bq([
            "cp",
            f"{self.project_id}:{self.dataset_id}.{table_id}",
            f"{self.project_id}:{self.dataset_id}.{backup_name}"
        ], check=False)

        if result.returncode == 0:
            print(f"  [OK] Backup created")
            return backup_name
        else:
            print(f"  [WARN] Backup failed (table might be empty)")
            return ""

    def drop_table(self, table_id: str):
        print(f"  [DROP] Dropping table: {table_id}")
        self.run_bq([
            "rm", "-f", "-t", f"{self.project_id}:{self.dataset_id}.{table_id}"
        ], check=False)

    def restore_data(self, source_table: str, target_table: str, common_columns: List[str]):
        if not common_columns:
            print(f"  [WARN] No common columns to restore")
            return

        columns_sql = ", ".join(common_columns)
        query = f"""
        INSERT INTO `{self.project_id}.{self.dataset_id}.{target_table}` ({columns_sql})
        SELECT {columns_sql} FROM `{self.project_id}.{self.dataset_id}.{source_table}`
        """

        print(f"  [RESTORE] Restoring data from backup...")
        result = self.run_bq([
            "query", "--use_legacy_sql=false", query
        ], check=False)

        if result.returncode == 0:
            print(f"  [OK] Data restored")
            query = f"""
            DROP TABLE IF EXISTS `{self.project_id}.{self.dataset_id}.{source_table}`
            """
            print(f"  [DROP TEMP] Dropping backup table...")
            drop_backup_result = self.run_bq([
                "query", "--use_legacy_sql=false", query
            ], check=False)
            if drop_backup_result.returncode == 0:
                print(f"  [OK] Drop Backup table successful")
            else:
                print(f"  [WARN] Drop Backup table failed - manual intervention needed")
        else:
            print(f"  [WARN] Data restore failed - manual intervention needed")
            print(f"      Backup table: {source_table}")

    def execute_sql(self, sql: str) -> bool:
        print(f"  [SQL] Executing SQL:\n{sql}")
        result = self.run_bq([
            "query", "--use_legacy_sql=false", sql
        ], check=False)
        print(f" execute_sql result : {result}")
        if result.returncode != 0:
            print(f"  [SQL ERROR] stdout: {result.stdout}")
            print(f"  [SQL ERROR] stderr: {result.stderr}")
        return result.returncode == 0

    def deploy_table(self, definition: Dict, force: bool = False) -> bool:
        table_id = definition["table_id"]
        table_type = definition.get("table_type", "native")

        print(f"\n{'=' * 50}")
        print(f"Table: {table_id} ({table_type})")
        print(f"{'=' * 50}")

        if not self.table_exists(table_id):
            print(f"  [NEW] Table does not exist. Creating...")
            sql = self.generate_create_sql(definition)
            if self.execute_sql(sql):
                print(f"  [OK] Created!")
                return True
            else:
                print(f"  [ERROR] Failed to create!")
                return False

        print(f"  [INFO] Table exists. Checking for changes...")
        current = self.get_current_metadata(table_id)

        if not current:
            print(f"  [ERROR] Failed to get current metadata")
            return False

        change = self.compare_schemas(current, definition)

        if change.change_type == ChangeType.NO_CHANGE:
            print(f"  [OK] No changes. SKIPPING.")
            return True

        elif change.change_type == ChangeType.ADDITIVE:
            print(f"  [ADDITIVE] Additive changes detected:")
            for col in change.added_columns:
                print(f"      + {col}")

            new_cols = [col for col in definition["schema"] if col["name"] in change.added_columns]
            sql = self.generate_alter_sql(table_id, new_cols)

            print(f"  [ALTER] Executing ALTER TABLE...")
            if self.execute_sql(sql):
                print(f"  [OK] Columns added!")
                return True
            else:
                print(f"  [ERROR] Failed to add columns!")
                return False

        else:
            print(f"  [BREAKING] BREAKING CHANGES detected:")
            for col in change.modified_columns:
                print(f"      ~ {col}")
            for col in change.removed_columns:
                print(f"      - {col}")
            for other in change.other_changes:
                print(f"      ! {other}")

            if table_type == "native":
                print(f"\n  [MIGRATE] Starting migration (Native table)...")

                backup_name = self.backup_table(table_id)

                self.drop_table(table_id)

                print(f"  [CREATE] Creating table with new schema...")
                sql = self.generate_create_sql(definition)
                if not self.execute_sql(sql):
                    print(f"  [ERROR] Failed to create table!")
                    print(f"  [WARN] Backup available: {backup_name}")
                    return False

                if backup_name:
                    expected_cols = {col["name"] for col in definition["schema"]}
                    current_cols = {col["name"] for col in current.get("schema", {}).get("fields", [])}
                    common = list(expected_cols & current_cols)

                    self.restore_data(backup_name, table_id, common)

                print(f"  [OK] Migration complete!")
                return True

            else:
                print(f"\n  [MIGRATE] Starting migration (Iceberg table - data safe in GCS)...")

                self.drop_table(table_id)

                print(f"  [CREATE] Creating table with new schema...")
                sql = self.generate_create_sql(definition)
                if not self.execute_sql(sql):
                    print(f"  [ERROR] Failed to create table!")
                    return False

                print(f"  [OK] Migration complete!")
                return True


def main():
    if len(sys.argv) < 4:
        print("Usage: python deploy.py <PROJECT_ID> <DATASET_ID> <ENV> [--force]")
        print("Example: python deploy.py the1-insight-stg insight stg")
        sys.exit(1)

    project_id = sys.argv[1]
    dataset_id = sys.argv[2]
    env = sys.argv[3]
    force = "--force" in sys.argv

    script_dir = os.path.dirname(os.path.abspath(__file__))

    print("=" * 60)
    print("BigQuery Table Deployer")
    print("=" * 60)
    print(f"Project:  {project_id}")
    print(f"Dataset:  {dataset_id}")
    print(f"Env:      {env}")
    print(f"Force:    {force}")
    print(f"Path:     {os.path.abspath(__file__)}")
    print(f"Script:   {script_dir}")
    print("=" * 60)

    deployer = TableDeployer(project_id, dataset_id, env)

    json_files = sorted([f for f in os.listdir(script_dir) if f.endswith(".json")])

    if not json_files:
        print("[ERROR] No JSON definition files found!")
        sys.exit(1)

    print(f"\nFound {len(json_files)} table definitions")

    success_count = 0
    fail_count = 0

    for json_file in json_files:
        filepath = os.path.join(script_dir, json_file)

        try:
            with open(filepath, "r") as f:
                definition = json.load(f)
        except Exception as e:
            print(f"\n[ERROR] Failed to read {json_file}: {e}")
            fail_count += 1
            continue

        if deployer.deploy_table(definition, force):
            success_count += 1
        else:
            fail_count += 1

    print("\n" + "=" * 60)
    print(f"Deployment complete!")
    print(f"   Success: {success_count}")
    print(f"   Failed:  {fail_count}")
    print("=" * 60)

    sys.exit(0 if fail_count == 0 else 1)


if __name__ == "__main__":
    main()
