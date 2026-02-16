"""
Region Boundary DAG - Monthly ISO 3166-2 region boundary extraction.

Schedule: Monthly 1st at 06:00 UTC
Consumes: planet GOL from R2

Pipeline:
1. Extract all ISO 3166-2 region boundaries from OSM via gol query
2. Dissolve per region into single geometry (Docker ogr2ogr)
3. Export GeoPackage with geodesic area via ST_Area (Docker ogr2ogr)
4. Export GeoJSON and GeoParquet in parallel (Docker ogr2ogr)
5. Upload all formats to R2
"""

from __future__ import annotations

import os
import shutil
from datetime import timedelta

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
DISSOLVED_GPKG = f"{WORK_DIR}/dissolved.gpkg"
OUTPUT_BASENAME = f"{WORK_DIR}/regions-latest.boundary"
OUTPUT_GPKG = f"{OUTPUT_BASENAME}.gpkg"
OUTPUT_GEOJSON = f"{OUTPUT_BASENAME}.geojson"
OUTPUT_PARQUET = f"{OUTPUT_BASENAME}.parquet"


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

    extract = DockerOperator(
        task_id="extract_regions",
        task_display_name="Extract Regions",
        image=OPENPLANETDATA_IMAGE,
        command=["bash", "-c", f'gol query {SHARED_PLANET_OSM_GOL_PATH} \'a["ISO3166-2"]\' -f geojson > {RAW_GEOJSON}'],
        auto_remove="success",
        force_pull=True,
        mount_tmp_dir=False,
        mounts=[Mount(**DOCKER_MOUNT)],
        user=DOCKER_USER,
    )

    dissolve = Ogr2OgrOperator(
        task_id="dissolve_regions",
        task_display_name="Dissolve Regions",
        args=[
            "-f", "GPKG", DISSOLVED_GPKG, RAW_GEOJSON,
            "-dialect", "sqlite",
            "-sql", 'SELECT "ISO3166-2", ST_Union(geom) AS geom FROM raw GROUP BY "ISO3166-2"',
            "-nln", "dissolved",
        ],
    )

    export_gpkg = Ogr2OgrOperator(
        task_id="export_gpkg",
        task_display_name="Export GeoPackage",
        args=[
            "-f", "GPKG", OUTPUT_GPKG, DISSOLVED_GPKG,
            "-dialect", "sqlite",
            "-sql", 'SELECT geom, "ISO3166-2", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved',
            "-nln", "regions",
        ],
    )

    export_geojson = Ogr2OgrOperator(
        task_id="export_geojson",
        task_display_name="Export GeoJSON",
        environment={"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
        args=[
            "-f", "GeoJSON", OUTPUT_GEOJSON,
            OUTPUT_GPKG, "regions",
            "-nln", "regions",
        ],
    )

    export_parquet = Ogr2OgrOperator(
        task_id="export_parquet",
        task_display_name="Export GeoParquet",
        args=[
            "-f", "Parquet", OUTPUT_PARQUET,
            OUTPUT_GPKG, "regions",
            "-nln", "regions",
        ],
    )

    @task(task_display_name="Upload File")
    def upload_file(
        source: str,
        ext: str,
        media_type: str,
        subfolder: str,
    ) -> dict:
        """Upload a single format variant to R2."""
        hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)
        return hook.upload(
            bucket=R2_BUCKET,
            category="boundary",
            destination_filename=f"regions-latest.boundary.{ext}",
            destination_path=f"boundaries/regions/{subfolder}",
            destination_version="v1",
            entity="regions",
            extension=ext,
            media_type=media_type,
            source=source,
            tags=REGION_TAGS + [subfolder],
        )

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
    dirs >> gol_dl >> extract >> dissolve >> export_gpkg

    export_gpkg >> [export_geojson, export_parquet]

    upload_gpkg = upload_file.override(task_display_name="Upload GeoPackage")(OUTPUT_GPKG, "gpkg", "application/geopackage+sqlite3", "geopackage")
    upload_geojson = upload_file.override(task_display_name="Upload GeoJSON")(OUTPUT_GEOJSON, "geojson", "application/geo+json", "geojson")
    upload_parquet = upload_file.override(task_display_name="Upload GeoParquet")(OUTPUT_PARQUET, "parquet", "application/vnd.apache.parquet", "geoparquet")

    export_gpkg >> upload_gpkg
    export_geojson >> upload_geojson
    export_parquet >> upload_parquet

    [upload_gpkg, upload_geojson, upload_parquet] >> done()
    [upload_gpkg, upload_geojson, upload_parquet] >> cleanup()
