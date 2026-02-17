"""
Region Boundary DAG - Monthly ISO 3166-2 region boundary extraction.

Schedule: Monthly 1st at 06:00 UTC
Consumes: planet GOL from R2

Pipeline (per region):
1. Extract all ISO 3166-2 region boundaries from OSM via gol query
2. Parse regions to get list of ISO3166-2 codes
3. For each region in parallel:
   a. Extract region boundary (split into individual GeoJSON files)
   b. Clip coastline land polygons to region boundary (Ogr2OgrOperator)
   c. Dissolve clipped polygons into single geometry (Ogr2OgrOperator)
   d. Export GeoPackage with geodesic area via ST_Area (Ogr2OgrOperator)
   e. Export GeoJSON and GeoParquet in parallel (Ogr2OgrOperator)
   f. Upload all formats to R2
"""

from __future__ import annotations

import json
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
OPENSTREETMAP_REGIONS_GEOJSON = f"{WORK_DIR}/openstreetmap-regions.geojson"
COASTLINE_PATH = f"{WORK_DIR}/planet-latest.coastline.gpkg"


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
    max_active_tasks=32,
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

    @task.r2index_download(
        task_display_name="Download Planet Coastline",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_coastline() -> DownloadItem:
        """Download coastline GPKG from R2."""
        return DownloadItem(
            destination=COASTLINE_PATH,
            source_filename="planet-latest.coastline.gpkg",
            source_path="boundaries/coastline/geopackage",
            source_version="v1",
        )

    @task(task_display_name="Prepare Directory")
    def prepare_directory() -> None:
        """Create working directory."""
        os.makedirs(WORK_DIR, exist_ok=True)

    extract_all = DockerOperator(
        task_id="extract_all_regions",
        task_display_name="Extract All Regions",
        image=OPENPLANETDATA_IMAGE,
        command=["bash", "-c", f'gol query {SHARED_PLANET_OSM_GOL_PATH} \'a["ISO3166-2"]\' -f geojson > {OPENSTREETMAP_REGIONS_GEOJSON}'],
        auto_remove="success",
        force_pull=True,
        mount_tmp_dir=False,
        mounts=[Mount(**DOCKER_MOUNT)],
        user=DOCKER_USER,
    )

    @task(task_display_name="Split Regions into Files")
    def split_regions_into_files() -> list[str]:
        """Split raw GeoJSON into individual region files, returns sorted list of safe_codes."""
        regions_dir = f"{WORK_DIR}/split"
        os.makedirs(regions_dir, exist_ok=True)

        region_features: dict[str, list] = {}

        with open(OPENSTREETMAP_REGIONS_GEOJSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        if data.get("type") == "FeatureCollection":
            for feature in data.get("features", []):
                iso_code = feature.get("properties", {}).get("ISO3166-2")
                if not iso_code:
                    continue
                safe_code = iso_code.replace(":", "-")
                region_features.setdefault(safe_code, []).append(feature)

        for safe_code, features in region_features.items():
            region_file = f"{WORK_DIR}/split/{safe_code}.geojson"
            with open(region_file, "w", encoding="utf-8") as fh:
                json.dump({"type": "FeatureCollection", "features": features}, fh)

        return sorted(region_features.keys())

    @task(task_display_name="Prepare Region Directories")
    def prepare_region_dirs(codes: list[str]) -> None:
        """Create output directories for each region."""
        for code in codes:
            os.makedirs(f"{WORK_DIR}/{code}", exist_ok=True)

    @task
    def make_clip_args(codes: list[str]) -> list[dict]:
        return [
            {
                "args": [
                    "-f", "GPKG", f"{WORK_DIR}/{code}/clipped.gpkg",
                    COASTLINE_PATH, "land_polygons",
                    "-clipsrc", f"{WORK_DIR}/split/{code}.geojson",
                    "-nln", "clipped",
                ],
            }
            for code in codes
        ]

    @task
    def make_dissolve_args(codes: list[str]) -> list[dict]:
        return [
            {
                "args": [
                    "-f", "GPKG", f"{WORK_DIR}/{code}/dissolved.gpkg",
                    f"{WORK_DIR}/{code}/clipped.gpkg",
                    "-dialect", "sqlite",
                    "-sql", "SELECT ST_Union(geom) AS geom FROM clipped",
                    "-nln", "dissolved",
                ],
            }
            for code in codes
        ]

    @task
    def make_export_gpkg_args(codes: list[str]) -> list[dict]:
        return [
            {
                "args": [
                    "-f", "GPKG", f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg",
                    f"{WORK_DIR}/{code}/dissolved.gpkg",
                    "-dialect", "sqlite",
                    "-sql", f"""SELECT geom, '{code.replace("-", ":", 1)}' AS "ISO3166-2", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved""",
                    "-nln", code,
                ],
            }
            for code in codes
        ]

    @task
    def make_export_geojson_args(codes: list[str]) -> list[dict]:
        return [
            {
                "environment": {"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
                "args": [
                    "-f", "GeoJSON", f"{WORK_DIR}/{code}/{code}-latest.boundary.geojson",
                    f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg", code,
                    "-nln", code,
                ],
            }
            for code in codes
        ]

    @task
    def make_export_parquet_args(codes: list[str]) -> list[dict]:
        return [
            {
                "args": [
                    "-f", "Parquet", f"{WORK_DIR}/{code}/{code}-latest.boundary.parquet",
                    f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg", code,
                    "-nln", code,
                ],
            }
            for code in codes
        ]

    @task(task_display_name="Upload Region")
    def upload_region(code: str) -> dict:
        """Upload all formats for a single region to R2."""
        hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)
        region_dir = f"{WORK_DIR}/{code}"

        for ext, subfolder, media_type in [
            ("gpkg", "geopackage", "application/geopackage+sqlite3"),
            ("geojson", "geojson", "application/geo+json"),
            ("parquet", "geoparquet", "application/vnd.apache.parquet"),
        ]:
            source = f"{region_dir}/{code}-latest.boundary.{ext}"
            if not os.path.exists(source):
                raise FileNotFoundError(f"Missing output: {source}")
            hook.upload(
                bucket=R2_BUCKET,
                category="boundary",
                destination_filename=f"{code}-latest.boundary.{ext}",
                destination_path=f"boundaries/regions/{code}/{subfolder}",
                destination_version="v1",
                entity=code.replace("-", ":", 1),
                extension=ext,
                media_type=media_type,
                source=source,
                tags=REGION_TAGS + [code, subfolder],
            )
        return {"code": code, "status": "uploaded"}

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
    coastline_dl = download_coastline()
    dirs >> [gol_dl, coastline_dl]
    gol_dl >> extract_all

    codes = split_regions_into_files()
    extract_all >> codes

    region_dirs = prepare_region_dirs(codes)

    clip = Ogr2OgrOperator.partial(
        task_id="clip_coastline",
        task_display_name="Clip Coastline",
    ).expand_kwargs(make_clip_args(codes))

    dissolve = Ogr2OgrOperator.partial(
        task_id="dissolve",
        task_display_name="Dissolve",
    ).expand_kwargs(make_dissolve_args(codes))

    export_gpkg = Ogr2OgrOperator.partial(
        task_id="export_gpkg",
        task_display_name="Export GeoPackage",
    ).expand_kwargs(make_export_gpkg_args(codes))

    export_geojson = Ogr2OgrOperator.partial(
        task_id="export_geojson",
        task_display_name="Export GeoJSON",
    ).expand_kwargs(make_export_geojson_args(codes))

    export_parquet = Ogr2OgrOperator.partial(
        task_id="export_parquet",
        task_display_name="Export GeoParquet",
    ).expand_kwargs(make_export_parquet_args(codes))

    upload_results = upload_region.expand(code=codes)

    # Dependencies
    [region_dirs, coastline_dl] >> clip >> dissolve >> export_gpkg >> [export_geojson, export_parquet] >> upload_results

    upload_results >> done()
    upload_results >> cleanup()
