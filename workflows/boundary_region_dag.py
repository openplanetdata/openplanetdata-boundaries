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

    @task(task_display_name="Parse Region Codes")
    def parse_region_codes() -> list[str]:
        """Extract unique ISO3166-2 codes from the raw GeoJSON."""
        with open(RAW_GEOJSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        codes = set()
        if data.get("type") == "FeatureCollection":
            for feature in data.get("features", []):
                iso_code = feature.get("properties", {}).get("ISO3166-2")
                if iso_code:
                    codes.add(iso_code)

        return sorted(codes)

    @task(task_display_name="Prepare Region Directory")
    def prepare_region_directory(region_code: str) -> str:
        """Create directory for a specific region."""
        # Sanitize region code for filesystem (replace : with -)
        safe_code = region_code.replace(":", "-")
        region_dir = f"{WORK_DIR}/{safe_code}"
        os.makedirs(region_dir, exist_ok=True)
        return safe_code

    @task(task_display_name="Extract Region")
    def extract_region(region_code: str, safe_code: str) -> str:
        """Extract a single region boundary from the raw GeoJSON."""
        region_geojson = f"{WORK_DIR}/{safe_code}/raw.geojson"

        # Read raw geojson and filter for this region
        with open(RAW_GEOJSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        features = []
        if data.get("type") == "FeatureCollection":
            for feature in data.get("features", []):
                if feature.get("properties", {}).get("ISO3166-2") == region_code:
                    features.append(feature)

        filtered_data = {
            "type": "FeatureCollection",
            "features": features
        }

        with open(region_geojson, "w", encoding="utf-8") as fh:
            json.dump(filtered_data, fh)

        return region_geojson

    @task(task_display_name="Process Region")
    def process_region(region_code: str, safe_code: str, region_geojson: str) -> dict[str, str]:
        """Process a single region: dissolve and export to all formats."""
        from airflow.sdk import get_current_context

        dissolved_gpkg = f"{WORK_DIR}/{safe_code}/dissolved.gpkg"
        output_gpkg = f"{WORK_DIR}/{safe_code}/{safe_code}-latest.boundary.gpkg"
        output_geojson = f"{WORK_DIR}/{safe_code}/{safe_code}-latest.boundary.geojson"
        output_parquet = f"{WORK_DIR}/{safe_code}/{safe_code}-latest.boundary.parquet"

        safe_region_code = region_code.replace("'", "''")

        context = get_current_context()

        # Dissolve using ogr2ogr in Docker
        dissolve_op = Ogr2OgrOperator(
            task_id=f"dissolve_{safe_code}",
            args=[
                "-f", "GPKG", dissolved_gpkg, region_geojson,
                "-dialect", "sqlite",
                "-sql", "SELECT ST_Union(geometry) AS geom FROM raw",
                "-nln", "dissolved",
            ],
        )
        dissolve_op.execute(context)

        # Export GPKG with attributes
        export_gpkg_op = Ogr2OgrOperator(
            task_id=f"export_gpkg_{safe_code}",
            args=[
                "-f", "GPKG", output_gpkg, dissolved_gpkg,
                "-dialect", "sqlite",
                "-sql", f"""SELECT geom, '{safe_region_code}' AS "ISO3166-2", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved""",
                "-nln", safe_code,
            ],
        )
        export_gpkg_op.execute(context)

        # Export GeoJSON
        export_geojson_op = Ogr2OgrOperator(
            task_id=f"export_geojson_{safe_code}",
            environment={"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
            args=[
                "-f", "GeoJSON", output_geojson,
                output_gpkg, safe_code,
                "-nln", safe_code,
            ],
        )
        export_geojson_op.execute(context)

        # Export GeoParquet
        export_parquet_op = Ogr2OgrOperator(
            task_id=f"export_parquet_{safe_code}",
            args=[
                "-f", "Parquet", output_parquet,
                output_gpkg, safe_code,
                "-nln", safe_code,
            ],
        )
        export_parquet_op.execute(context)

        return {
            "gpkg": output_gpkg,
            "geojson": output_geojson,
            "parquet": output_parquet,
        }

    @task(task_display_name="Upload Region Files")
    def upload_region_files(
        region_code: str,
        safe_code: str,
        outputs: dict[str, str],
    ) -> dict[str, dict]:
        """Upload all format variants for a region to R2."""
        hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)

        results = {}

        # Upload GPKG
        results["gpkg"] = hook.upload(
            bucket=R2_BUCKET,
            category="boundary",
            destination_filename=f"{safe_code}-latest.boundary.gpkg",
            destination_path=f"boundaries/regions/{safe_code}/geopackage",
            destination_version="v1",
            entity=safe_code,
            extension="gpkg",
            media_type="application/geopackage+sqlite3",
            source=outputs["gpkg"],
            tags=REGION_TAGS + [safe_code, "geopackage"],
        )

        # Upload GeoJSON
        results["geojson"] = hook.upload(
            bucket=R2_BUCKET,
            category="boundary",
            destination_filename=f"{safe_code}-latest.boundary.geojson",
            destination_path=f"boundaries/regions/{safe_code}/geojson",
            destination_version="v1",
            entity=safe_code,
            extension="geojson",
            media_type="application/geo+json",
            source=outputs["geojson"],
            tags=REGION_TAGS + [safe_code, "geojson"],
        )

        # Upload GeoParquet
        results["parquet"] = hook.upload(
            bucket=R2_BUCKET,
            category="boundary",
            destination_filename=f"{safe_code}-latest.boundary.parquet",
            destination_path=f"boundaries/regions/{safe_code}/geoparquet",
            destination_version="v1",
            entity=safe_code,
            extension="parquet",
            media_type="application/vnd.apache.parquet",
            source=outputs["parquet"],
            tags=REGION_TAGS + [safe_code, "geoparquet"],
        )

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

    # Parse region codes from extracted data
    region_codes = parse_region_codes()
    extract_all >> region_codes

    # Dynamic task mapping - process each region in parallel
    safe_codes = prepare_region_directory.expand(region_code=region_codes)

    # Extract individual region data
    region_geojsons = extract_region.expand(
        region_code=region_codes,
        safe_code=safe_codes
    )

    # Process each region (dissolve + export all formats)
    region_outputs = process_region.expand(
        region_code=region_codes,
        safe_code=safe_codes,
        region_geojson=region_geojsons
    )

    # Upload all formats for each region
    uploads = upload_region_files.expand(
        region_code=region_codes,
        safe_code=safe_codes,
        outputs=region_outputs
    )

    # Final tasks
    uploads >> done()
    uploads >> cleanup()
