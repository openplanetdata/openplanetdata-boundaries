"""
Region Boundary DAG - Monthly ISO 3166-2 region boundary extraction.

Schedule: Monthly 1st at 06:00 UTC
Consumes: planet GOL from R2

Pipeline (per region):
1. Extract all ISO 3166-2 region boundaries from OSM via gol query
2. Parse regions to get list of ISO3166-2 codes
3. For each region in parallel (independent chain):
   a. Extract region boundary (split into individual GeoJSON files)
   b. Clip coastline land polygons to region boundary (Docker ogr2ogr)
   c. Dissolve clipped polygons into single geometry (Docker ogr2ogr)
   d. Export GeoPackage with geodesic area via ST_Area (Docker ogr2ogr)
   e. Export GeoJSON and GeoParquet in parallel (Docker ogr2ogr)
   f. Upload all formats to R2
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import timedelta

from airflow.sdk import DAG, task, task_group

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.defaults import (
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_COASTLINE_GPKG_PATH,
    SHARED_PLANET_OSM_GOL_PATH,
)
from openplanetdata.airflow.operators.gol import GolOperator
from openplanetdata.airflow.operators.ogr2ogr import Ogr2OgrOperator

REGION_TAGS = ["boundaries", "regions", "openplanetdata"]

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/regions"
OPENSTREETMAP_REGIONS_GEOJSON = f"{WORK_DIR}/openstreetmap-regions.geojson"


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
    description="ISO3166-2 region boundary extraction from OSM",
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=64,
    schedule="0 6 1 * *",
    tags=["boundaries", "regions", "openplanetdata"],
) as dag:

    @task.r2index_download(
        task_display_name="Download Planet Coastline",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_coastline() -> DownloadItem:
        """Download coastline GPKG from R2."""
        return DownloadItem(
            destination=SHARED_PLANET_COASTLINE_GPKG_PATH,
            source_filename="planet-latest.coastline.gpkg",
            source_path="boundaries/coastline/geopackage",
            source_version="v1",
        )

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
    def ensure_work_dir_exists() -> None:
        """Create working directory."""
        os.makedirs(WORK_DIR, exist_ok=True)

    extract_region_boundaries_from_osm = GolOperator(
        task_id="extract_all_regions_from_osm",
        task_display_name="Extract All ISO3166-2 Boundaries from OSM",
        args=["query", SHARED_PLANET_OSM_GOL_PATH, 'a["ISO3166-2"]', "-f", "geojson"],
        output_file=OPENSTREETMAP_REGIONS_GEOJSON,
    )

    @task(task_display_name="Split Regions into Files")
    def split_regions_into_files() -> list[str]:
        """Split raw GeoJSON into individual region files, returns sorted list of osm_region_codes."""
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

    @task_group(group_display_name="Process Region")
    def process_region(code: str) -> None:
        """Per-region independent pipeline: clip → dissolve → export → upload."""

        clip = Ogr2OgrOperator(
            task_id="clip_coastline",
            task_display_name="Clip Coastline",
            args=[
                "-f", "GPKG", f"{WORK_DIR}/{code}/clipped.gpkg",
                SHARED_PLANET_COASTLINE_GPKG_PATH, "land_polygons",
                "-clipsrc", f"{WORK_DIR}/split/{code}.geojson",
                "-makevalid",
                "-nln", "clipped",
            ],
        )

        dissolve = Ogr2OgrOperator(
            task_id="dissolve",
            task_display_name="Dissolve",
            args=[
                "-f", "GPKG", f"{WORK_DIR}/{code}/dissolved.gpkg",
                f"{WORK_DIR}/{code}/clipped.gpkg",
                "-dialect", "sqlite",
                "-sql", "SELECT ST_Union(geom) AS geom FROM clipped",
                "-nln", "dissolved",
            ],
        )

        export_gpkg = Ogr2OgrOperator(
            task_id="export_gpkg",
            task_display_name="Export GeoPackage",
            args=[
                "-f", "GPKG", f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg",
                f"{WORK_DIR}/{code}/dissolved.gpkg",
                "-dialect", "sqlite",
                "-sql", """SELECT geom, '{{ code | replace("-", ":") }}' AS "ISO3166-2", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved""",
                "-nln", f"{code}",
            ],
        )

        export_geojson = Ogr2OgrOperator(
            task_id="export_geojson",
            task_display_name="Export GeoJSON",
            environment={"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
            args=[
                "-f", "GeoJSON", f"{WORK_DIR}/{code}/{code}-latest.boundary.geojson",
                f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg", f"{code}",
                "-nln", f"{code}",
            ],
        )

        export_parquet = Ogr2OgrOperator(
            task_id="export_parquet",
            task_display_name="Export GeoParquet",
            args=[
                "-f", "Parquet", f"{WORK_DIR}/{code}/{code}-latest.boundary.parquet",
                f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg", f"{code}",
                "-nln", f"{code}",
            ],
        )

        @task(task_display_name="Upload GeoPackage")
        def upload_gpkg(code: str) -> None:
            R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID).upload(
                bucket=R2_BUCKET,
                category="boundary",
                destination_filename=f"{code}-latest.boundary.gpkg",
                destination_path=f"boundaries/regions/{code}/geopackage",
                destination_version="v1",
                entity=code.replace("-", ":", 1),
                extension="gpkg",
                media_type="application/geopackage+sqlite3",
                source=f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg",
                tags=REGION_TAGS + [code, "geopackage"],
            )

        @task(task_display_name="Upload GeoJSON")
        def upload_geojson(code: str) -> None:
            R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID).upload(
                bucket=R2_BUCKET,
                category="boundary",
                destination_filename=f"{code}-latest.boundary.geojson",
                destination_path=f"boundaries/regions/{code}/geojson",
                destination_version="v1",
                entity=code.replace("-", ":", 1),
                extension="geojson",
                media_type="application/geo+json",
                source=f"{WORK_DIR}/{code}/{code}-latest.boundary.geojson",
                tags=REGION_TAGS + [code, "geojson"],
            )

        @task(task_display_name="Upload GeoParquet")
        def upload_parquet(code: str) -> None:
            R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID).upload(
                bucket=R2_BUCKET,
                category="boundary",
                destination_filename=f"{code}-latest.boundary.parquet",
                destination_path=f"boundaries/regions/{code}/geoparquet",
                destination_version="v1",
                entity=code.replace("-", ":", 1),
                extension="parquet",
                media_type="application/vnd.apache.parquet",
                source=f"{WORK_DIR}/{code}/{code}-latest.boundary.parquet",
                tags=REGION_TAGS + [code, "geoparquet"],
            )

        gpkg_upload = upload_gpkg(code)
        geojson_upload = upload_geojson(code)
        parquet_upload = upload_parquet(code)

        clip >> dissolve >> export_gpkg >> [export_geojson, export_parquet, gpkg_upload]
        export_geojson >> geojson_upload
        export_parquet >> parquet_upload

    @task(task_display_name="Report Failures", trigger_rule="all_done")
    def report_failures(codes: list[str]) -> None:
        """Report regions that failed to produce output files."""
        failed = [
            code for code in codes
            if not os.path.exists(f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg")
        ]
        if failed:
            print(f"Processing failed for {len(failed)}/{len(codes)} region(s):")
            for code in failed:
                print(f"  {code}")
        else:
            print(f"All {len(codes)} regions processed successfully.")

    @task(task_display_name="Done", trigger_rule="all_done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    dirs = ensure_work_dir_exists()
    gol_dl = download_planet_gol()
    coastline_dl = download_coastline()
    dirs >> [gol_dl, coastline_dl]
    gol_dl >> extract_region_boundaries_from_osm

    codes = split_regions_into_files()
    extract_region_boundaries_from_osm >> codes

    region_dirs = prepare_region_dirs(codes)

    process_groups = process_region.expand(code=codes)
    [region_dirs, coastline_dl] >> process_groups

    report = report_failures(codes)
    process_groups >> report >> done()
    process_groups >> cleanup()
