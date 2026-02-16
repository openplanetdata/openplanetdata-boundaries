"""
Region Boundary DAG - Monthly ISO 3166-2 region boundary extraction.

Schedule: Monthly 1st at 06:00 UTC
Consumes: planet GOL from R2

Pipeline (per region):
1. Extract all ISO 3166-2 region boundaries from OSM via gol query
2. Parse regions to get list of ISO3166-2 codes
3. For each region in parallel:
   a. Extract region boundary
   b. Dissolve into single geometry (Docker ogr2ogr)
   c. Export GeoPackage with geodesic area via ST_Area (Docker ogr2ogr)
   d. Export GeoJSON and GeoParquet in parallel (Docker ogr2ogr)
   e. Upload all formats to R2
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import timedelta
from typing import Any

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, task
from docker.types import Mount

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    OPENPLANETDATA_IMAGE,
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_GOL_PATH,
)
from openplanetdata.airflow.operators.ogr2ogr import DOCKER_USER, Ogr2OgrOperator

REGION_TAGS = ["boundaries", "regions", "openplanetdata"]

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/regions"
RAW_GEOJSON = f"{WORK_DIR}/raw.geojson"


with DAG(
    catchup=False,
    dag_display_name="OpenPlanetData Boundaries Regions",
    dag_id="openplanetdata_boundaries_regions",
    default_args={
        "execution_timeout": timedelta(hours=2),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
    },
    description="Monthly ISO 3166-2 region boundary extraction from OSM",
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=256,  # Allow processing many regions in parallel
    schedule="0 6 1 * *",
    tags=["boundaries", "regions", "openplanetdata"],
) as dag:

    @task.r2index_download(
        task_display_name="Download Planet GOL",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_planet_gol() -> DownloadItem:
        """Download planet GOL from R2."""
        return DownloadItem(
            destination=SHARED_PLANET_OSM_GOL_PATH,
            overwrite=False,
            source_filename="planet-latest.osm.gol",
            source_path="osm/planet/gol",
            source_version="v2",
        )

    @task(task_display_name="Prepare Directory")
    def prepare_directory() -> None:
        """Create working directory."""
        os.makedirs(WORK_DIR, exist_ok=True)

    extract_all = DockerOperator(
        task_id="extract_all_regions",
        task_display_name="Extract All Regions",
        image=OPENPLANETDATA_IMAGE,
        command=["bash", "-c", f'gol query {SHARED_PLANET_OSM_GOL_PATH} \'a["ISO3166-2"]\' -f geojson > {RAW_GEOJSON}'],
        auto_remove="success",
        force_pull=True,
        mount_tmp_dir=False,
        mounts=[Mount(**DOCKER_MOUNT)],
        user=DOCKER_USER,
    )

    @task(task_display_name="Split Regions into Files")
    def split_regions_into_files() -> dict[str, int]:
        """Split raw GeoJSON into individual region files to avoid memory issues."""
        regions_dir = f"{WORK_DIR}/split"
        os.makedirs(regions_dir, exist_ok=True)

        # Process raw GeoJSON and write individual region files
        region_files = {}

        with open(RAW_GEOJSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        if data.get("type") == "FeatureCollection":
            for feature in data.get("features", []):
                iso_code = feature.get("properties", {}).get("ISO3166-2")
                if not iso_code:
                    continue

                safe_code = iso_code.replace(":", "-")
                region_file = f"{regions_dir}/{safe_code}.geojson"

                # Append features to region file
                if safe_code not in region_files:
                    region_files[safe_code] = []

                region_files[safe_code].append(feature)

        # Write each region's features to a file
        for safe_code, features in region_files.items():
            region_file = f"{regions_dir}/{safe_code}.geojson"
            region_data = {
                "type": "FeatureCollection",
                "features": features
            }
            with open(region_file, "w", encoding="utf-8") as fh:
                json.dump(region_data, fh)

        return {"total_regions": len(region_files)}

    @task(task_display_name="Create Region Batches")
    def create_region_batches(split_info: dict[str, int], batch_size: int = 50) -> list[dict[str, Any]]:
        """Create batches of region codes for parallel processing."""
        regions_dir = f"{WORK_DIR}/split"

        # Get all region files
        region_codes = []
        for filename in sorted(os.listdir(regions_dir)):
            if filename.endswith(".geojson"):
                region_codes.append(filename[:-8])  # Remove .geojson extension

        # Create batches
        batches = []
        for i in range(0, len(region_codes), batch_size):
            batch_codes = region_codes[i:i + batch_size]
            batches.append({
                "batch_id": i // batch_size,
                "codes": batch_codes,
            })

        return batches

    @task(task_display_name="Create Batch Processing Script")
    def create_batch_script(batch: dict[str, Any]) -> str:
        """Create a shell script to process a batch of regions using ogr2ogr."""
        batch_id = batch["batch_id"]
        safe_codes = batch["codes"]

        script_path = f"{WORK_DIR}/batch_{batch_id}.sh"

        script_content = f"""#!/bin/bash
set -e

REGIONS_DIR="{WORK_DIR}/split"
WORK_DIR="{WORK_DIR}"

process_region() {{
    local SAFE_CODE=$1
    local REGION_CODE=$(echo $SAFE_CODE | sed 's/-/:/' )
    local REGION_DIR="$WORK_DIR/$SAFE_CODE"

    mkdir -p "$REGION_DIR"

    local REGION_GEOJSON="$REGIONS_DIR/$SAFE_CODE.geojson"
    local DISSOLVED_GPKG="$REGION_DIR/dissolved.gpkg"
    local OUTPUT_GPKG="$REGION_DIR/$SAFE_CODE-latest.boundary.gpkg"
    local OUTPUT_GEOJSON="$REGION_DIR/$SAFE_CODE-latest.boundary.geojson"
    local OUTPUT_PARQUET="$REGION_DIR/$SAFE_CODE-latest.boundary.parquet"

    # Dissolve
    ogr2ogr -f GPKG "$DISSOLVED_GPKG" "$REGION_GEOJSON" \\
        -dialect sqlite \\
        -sql "SELECT ST_Union(geometry) AS geom FROM \\"$SAFE_CODE\\"" \\
        -nln dissolved

    # Export GPKG with attributes
    ogr2ogr -f GPKG "$OUTPUT_GPKG" "$DISSOLVED_GPKG" \\
        -dialect sqlite \\
        -sql "SELECT geom, '$REGION_CODE' AS \\"ISO3166-2\\", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved" \\
        -nln "$SAFE_CODE"

    # Export GeoJSON
    OGR_GEOJSON_MAX_OBJ_SIZE=0 ogr2ogr -f GeoJSON "$OUTPUT_GEOJSON" "$OUTPUT_GPKG" "$SAFE_CODE" -nln "$SAFE_CODE"

    # Export GeoParquet
    ogr2ogr -f Parquet "$OUTPUT_PARQUET" "$OUTPUT_GPKG" "$SAFE_CODE" -nln "$SAFE_CODE"

    echo "Processed: $SAFE_CODE"
}}

"""
        # Add region processing calls
        for safe_code in safe_codes:
            script_content += f'process_region "{safe_code}"\n'

        with open(script_path, "w") as f:
            f.write(script_content)

        os.chmod(script_path, 0o755)
        return script_path

    def process_batch_docker_op(batch_id: int, script_path: str) -> DockerOperator:
        """Create a DockerOperator to run the batch processing script."""
        return DockerOperator(
            task_id=f"process_batch_{batch_id}",
            task_display_name=f"Process Batch {batch_id}",
            image="ghcr.io/osgeo/gdal:ubuntu-full-latest",
            command=["bash", script_path],
            auto_remove="success",
            mount_tmp_dir=False,
            mounts=[Mount(**DOCKER_MOUNT)],
            user=DOCKER_USER,
        )

    @task(task_display_name="Upload Region Batch")
    def upload_region_batch(batch: dict[str, Any]) -> dict[str, Any]:
        """Upload all regions in a batch to R2."""
        batch_id = batch["batch_id"]
        safe_codes = batch["codes"]
        hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)

        results = {
            "batch_id": batch_id,
            "uploaded": 0,
            "failed": 0,
        }

        for safe_code in safe_codes:
            try:
                region_code = safe_code.replace("-", ":", 1)
                region_dir = f"{WORK_DIR}/{safe_code}"

                output_gpkg = f"{region_dir}/{safe_code}-latest.boundary.gpkg"
                output_geojson = f"{region_dir}/{safe_code}-latest.boundary.geojson"
                output_parquet = f"{region_dir}/{safe_code}-latest.boundary.parquet"

                if not os.path.exists(output_gpkg):
                    results["failed"] += 1
                    continue

                # Upload all formats
                hook.upload(
                    bucket=R2_BUCKET,
                    category="boundary",
                    destination_filename=f"{safe_code}-latest.boundary.gpkg",
                    destination_path=f"boundaries/regions/{safe_code}/geopackage",
                    destination_version="v1",
                    entity=safe_code,
                    extension="gpkg",
                    media_type="application/geopackage+sqlite3",
                    source=output_gpkg,
                    tags=REGION_TAGS + [safe_code, "geopackage"],
                )

                hook.upload(
                    bucket=R2_BUCKET,
                    category="boundary",
                    destination_filename=f"{safe_code}-latest.boundary.geojson",
                    destination_path=f"boundaries/regions/{safe_code}/geojson",
                    destination_version="v1",
                    entity=safe_code,
                    extension="geojson",
                    media_type="application/geo+json",
                    source=output_geojson,
                    tags=REGION_TAGS + [safe_code, "geojson"],
                )

                hook.upload(
                    bucket=R2_BUCKET,
                    category="boundary",
                    destination_filename=f"{safe_code}-latest.boundary.parquet",
                    destination_path=f"boundaries/regions/{safe_code}/geoparquet",
                    destination_version="v1",
                    entity=safe_code,
                    extension="parquet",
                    media_type="application/vnd.apache.parquet",
                    source=output_parquet,
                    tags=REGION_TAGS + [safe_code, "geoparquet"],
                )

                results["uploaded"] += 1

            except Exception as e:
                results["failed"] += 1
                print(f"Failed to upload region {safe_code}: {e}")

        return results

    @task(task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    dirs = prepare_directory()
    gol_dl = download_planet_gol()
    dirs >> gol_dl >> extract_all

    # Split regions into individual files to avoid loading large file in each batch
    split_info = split_regions_into_files()
    extract_all >> split_info

    # Create batches of regions for parallel processing
    batches = create_region_batches(split_info)

    # Create processing scripts for each batch
    scripts = create_batch_script.expand(batch=batches)

    # Process each batch using DockerOperator (similar to extract task)
    # Note: We can't use .expand() on DockerOperator directly, so we create them dynamically
    # For now, use a task to orchestrate
    @task(task_display_name="Process All Batches")
    def process_all_batches(batch_scripts: list[tuple[dict, str]]) -> list[dict]:
        """Run all batch processing scripts."""
        results = []
        for batch, script_path in batch_scripts:
            batch_id = batch["batch_id"]
            op = process_batch_docker_op(batch_id, script_path)
            op.execute({})
            results.append({"batch_id": batch_id, "status": "completed"})
        return results

    # Process batches
    batch_info = process_all_batches(list(zip(batches, scripts)))

    # Upload results for each batch
    upload_results = upload_region_batch.expand(batch=batches)
    batch_info >> upload_results

    # Final tasks
    upload_results >> done()
    upload_results >> cleanup()
