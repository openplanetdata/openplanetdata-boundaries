"""
Aggregate Metadata DAG - Aggregates metadata from all boundary files.

Schedule: Monthly 1st at 08:00 UTC
Source: .github/workflows/aggregate-boundaries-metadata.yaml

This DAG has been simplified to use the r2index API instead of
iterating through .metadata files manually.

Tasks:
1. fetch_and_aggregate - Single API call to get all file metadata
2. upload_index - Upload aggregated manifest to R2
"""

from __future__ import annotations

import json
import os
import tempfile
from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import UploadItem

from workflows.config import (
    BOUNDARIES_BASE_PATH,
    POD_CONFIG_DEFAULT,
    R2_BUCKET,
    R2_CONN_ID,
)

default_args = {
    "depends_on_past": False,
    "execution_timeout": timedelta(minutes=30),
    "owner": "openplanetdata",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    catchup=False,
    dag_id="aggregate_metadata",
    default_args=default_args,
    description="Aggregate metadata from all boundary files",
    doc_md=__doc__,
    schedule="0 8 1 * *",
    start_date=datetime(2024, 1, 1),
    tags=["metadata", "aggregation", "monthly"],
) as dag:

    @task(executor_config={"resources": POD_CONFIG_DEFAULT})
    def fetch_and_aggregate() -> str:
        """
        Fetch all file metadata from r2index API and aggregate.

        This replaces the previous approach of:
        1. list_all_metadata_files() - List .metadata files via rclone
        2. download_and_aggregate() - Download each file and parse

        Now done in a single API call.

        Returns:
            Path to aggregated metadata file.
        """
        work_dir = tempfile.mkdtemp(prefix="metadata_aggregate_")
        output_path = os.path.join(work_dir, "boundaries.metadata")

        # Single API call replaces iterating through .metadata files
        hook = R2IndexHook(r2index_conn_id=R2_CONN_ID)
        response = hook.list_files(bucket=R2_BUCKET, category="boundary")
        aggregated = response.get("files", [])

        # Sort keys within each entry for consistent output
        sorted_aggregated = []
        for entry in aggregated:
            sorted_entry = dict(sorted(entry.items()))
            sorted_aggregated.append(sorted_entry)

        with open(output_path, "w") as f:
            json.dump(sorted_aggregated, f, indent=2)

        print(f"Aggregated {len(sorted_aggregated)} metadata entries from r2index API")
        return output_path

    @task.r2index_upload(
        bucket=R2_BUCKET,
        r2index_conn_id=R2_CONN_ID,
        executor_config={"resources": POD_CONFIG_DEFAULT},
    )
    def upload_index(metadata_path: str) -> UploadItem:
        """
        Upload aggregated metadata index to R2.

        Args:
            metadata_path: Path to aggregated metadata file.

        Returns:
            UploadItem for the aggregated metadata file.
        """
        return UploadItem(
            category="metadata",
            destination_filename="boundaries.metadata",
            destination_path=BOUNDARIES_BASE_PATH,
            destination_version="1",
            entity="boundaries-index",
            extension="metadata",
            media_type="application/json",
            source=metadata_path,
            tags=["metadata", "index"],
        )

    @task(
        trigger_rule="all_done",
        executor_config={"resources": POD_CONFIG_DEFAULT},
    )
    def cleanup(metadata_path: str) -> None:
        """Clean up temporary files."""
        import shutil

        work_dir = os.path.dirname(metadata_path)
        if work_dir and os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)

    # Simplified task flow: fetch -> upload -> cleanup
    aggregated_path = fetch_and_aggregate()
    upload_result = upload_index(aggregated_path)
    cleanup(aggregated_path)
