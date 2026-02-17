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
import shlex
import shutil
from datetime import timedelta

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, task, task_group
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
from openplanetdata.airflow.operators.ogr2ogr import DOCKER_USER

REGION_TAGS = ["boundaries", "regions", "openplanetdata"]

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/regions"
OPENSTREETMAP_REGIONS_GEOJSON = f"{WORK_DIR}/openstreetmap-regions.geojson"
COASTLINE_PATH = f"{WORK_DIR}/planet-latest.coastline.gpkg"
GDAL_IMAGE = "ghcr.io/osgeo/gdal:ubuntu-full-latest"


def _run_ogr2ogr(args: list[str], env: dict | None = None) -> None:
    """Run ogr2ogr in the GDAL Docker container, mirroring Ogr2OgrOperator."""
    import docker
    client = docker.from_env()
    cmd = shlex.join(["ogr2ogr", *args])
    client.containers.run(
        image=GDAL_IMAGE,
        command=f"bash -c {shlex.quote(cmd)}",
        environment=env or {},
        mounts=[Mount(**DOCKER_MOUNT)],
        remove=True,
        stderr=True,
        stdout=True,
        user=DOCKER_USER,
    )


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
    max_active_tasks=64,
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

    @task_group(group_display_name="Process Region")
    def process_region(code: str) -> None:
        """Per-region independent pipeline: clip → dissolve → export."""

        @task(task_display_name="Clip Coastline")
        def clip_coastline(code: str) -> str:
            _run_ogr2ogr([
                "-f", "GPKG", f"{WORK_DIR}/{code}/clipped.gpkg",
                COASTLINE_PATH, "land_polygons",
                "-clipsrc", f"{WORK_DIR}/split/{code}.geojson",
                "-makevalid",
                "-nln", "clipped",
            ])
            return code

        @task(task_display_name="Dissolve")
        def dissolve(code: str) -> str:
            _run_ogr2ogr([
                "-f", "GPKG", f"{WORK_DIR}/{code}/dissolved.gpkg",
                f"{WORK_DIR}/{code}/clipped.gpkg",
                "-dialect", "sqlite",
                "-sql", "SELECT ST_Union(geom) AS geom FROM clipped",
                "-nln", "dissolved",
            ])
            return code

        @task(task_display_name="Export GeoPackage")
        def export_gpkg(code: str) -> str:
            iso_code = code.replace("-", ":", 1)
            _run_ogr2ogr([
                "-f", "GPKG", f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg",
                f"{WORK_DIR}/{code}/dissolved.gpkg",
                "-dialect", "sqlite",
                "-sql", f"""SELECT geom, '{iso_code}' AS "ISO3166-2", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved""",
                "-nln", code,
            ])
            return code

        @task(task_display_name="Export GeoJSON")
        def export_geojson(code: str) -> str:
            _run_ogr2ogr(
                ["-f", "GeoJSON", f"{WORK_DIR}/{code}/{code}-latest.boundary.geojson",
                 f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg", code,
                 "-nln", code],
                env={"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
            )
            return code

        @task(task_display_name="Export GeoParquet")
        def export_parquet(code: str) -> str:
            _run_ogr2ogr([
                "-f", "Parquet", f"{WORK_DIR}/{code}/{code}-latest.boundary.parquet",
                f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg", code,
                "-nln", code,
            ])
            return code

        @task(task_display_name="Upload")
        def upload(code: str) -> None:
            hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)
            region_dir = f"{WORK_DIR}/{code}"
            for ext, subfolder, media_type in [
                ("gpkg", "geopackage", "application/geopackage+sqlite3"),
                ("geojson", "geojson", "application/geo+json"),
                ("parquet", "geoparquet", "application/vnd.apache.parquet"),
            ]:
                hook.upload(
                    bucket=R2_BUCKET,
                    category="boundary",
                    destination_filename=f"{code}-latest.boundary.{ext}",
                    destination_path=f"boundaries/regions/{code}/{subfolder}",
                    destination_version="v1",
                    entity=code.replace("-", ":", 1),
                    extension=ext,
                    media_type=media_type,
                    source=f"{region_dir}/{code}-latest.boundary.{ext}",
                    tags=REGION_TAGS + [code, subfolder],
                )

        clipped = clip_coastline(code)
        dissolved = dissolve(clipped)
        gpkg = export_gpkg(dissolved)
        geojson = export_geojson(gpkg)
        parquet = export_parquet(gpkg)
        upload([geojson, parquet])

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
    dirs = prepare_directory()
    gol_dl = download_planet_gol()
    coastline_dl = download_coastline()
    dirs >> [gol_dl, coastline_dl]
    gol_dl >> extract_all

    codes = split_regions_into_files()
    extract_all >> codes

    region_dirs = prepare_region_dirs(codes)

    process_groups = process_region.expand(code=codes)
    [region_dirs, coastline_dl] >> process_groups

    report = report_failures(codes)
    process_groups >> report >> done()
    process_groups >> cleanup()
