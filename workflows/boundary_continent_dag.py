"""
Continent Boundary DAG - Monthly continent boundary extraction.

Schedule: Monthly 1st at 05:00 UTC
Consumes: coastline GeoParquet from R2

Required tools on worker: ogr2ogr, python3 with pyproj

Continents: africa, antarctica, asia, europe, north-america, oceania, south-america

Per-continent pipeline:
1. Clip coastline by continent cookie-cutter (Docker ogr2ogr)
2. Dissolve clipped polygons into single geometry (Docker ogr2ogr)
3. Compute geodesic area and export final GeoPackage (worker ogr2ogr + pyproj)
4. Export GeoJSON and GeoParquet in parallel (Docker ogr2ogr)
5. Normalize GeoJSON to single Feature
6. Upload all formats to R2
"""

from __future__ import annotations

import os
import shutil
import subprocess
from datetime import timedelta
from airflow.exceptions import AirflowException
from airflow.sdk import DAG, task
from airflow.utils.task_group import TaskGroup

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.data.continents import CONTINENTS
from openplanetdata.airflow.defaults import OPENPLANETDATA_WORK_DIR, R2_BUCKET, R2INDEX_CONNECTION_ID
from workflows.operators.ogr2ogr import Ogr2OgrOperator

CONTINENT_TAGS = ["boundaries", "continents", "openplanetdata"]

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/continents"
COASTLINE_PATH = f"{WORK_DIR}/planet-latest.coastline.gpkg"
COOKIE_CUTTER_PATH = f"{WORK_DIR}/continent-cookie-cutter.gpkg"


with DAG(
    catchup=False,
    dag_display_name="OpenPlanetData Boundaries Continents",
    dag_id="openplanetdata_boundaries_continents",
    default_args={
        "execution_timeout": timedelta(hours=2),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
    },
    description="Monthly continent boundary extraction from OSM coastline",
    doc_md=__doc__,
    max_active_tasks=3,
    max_active_runs=1,
    schedule="0 5 1 * *",
    tags=["boundaries", "continents", "openplanetdata"],
) as dag:

    @task.r2index_download(
        task_display_name="Download Coastline",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_coastline() -> DownloadItem:
        """Download coastline data from R2."""
        return DownloadItem(
            destination=COASTLINE_PATH,
            source_filename="planet-latest.coastline.parquet",
            source_path="boundaries/coastline/geoparquet",
            source_version="v1",
        )

    @task.r2index_download(
        task_display_name="Download Cookie Cutter",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def download_cookie_cutter() -> DownloadItem:
        """Download continent cookie-cutter GPKG from R2."""
        return DownloadItem(
            destination=COOKIE_CUTTER_PATH,
            source_filename="continent-cookie-cutter.gpkg",
            source_path="boundaries/continents",
        )

    @task(task_display_name="Prepare Directories")
    def prepare_directories() -> None:
        """Create working directories for all continents."""
        os.makedirs(f"{WORK_DIR}/output", exist_ok=True)
        for continent in CONTINENTS:
            os.makedirs(f"{WORK_DIR}/{continent['slug']}", exist_ok=True)

    @task(task_display_name="Compute Area and Export GPKG")
    def compute_area_and_export_gpkg(
        slug: str,
        db_key: str,
        dissolved_path: str,
        output_gpkg: str,
    ) -> None:
        """Export dissolved geometry to GeoJSON, compute geodesic area, then export GPKG with area."""
        from workflows.utils.compute_area import compute_area_km2

        dissolved_geojson = dissolved_path.replace(".gpkg", ".geojson")

        subprocess.run(
            ["ogr2ogr", "-f", "GeoJSON", dissolved_geojson, dissolved_path, "dissolved"],
            check=True,
            env={**os.environ, "OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
        )

        area_km2 = compute_area_km2(dissolved_geojson)
        if area_km2 is None:
            raise AirflowException(f"Failed to compute geodesic area for {slug}")

        sql = f"SELECT geom, continent, CAST({area_km2} AS REAL) AS area FROM dissolved"
        subprocess.run(
            [
                "ogr2ogr", "-f", "GPKG", output_gpkg, dissolved_path,
                "-dialect", "sqlite", "-sql", sql, "-nln", slug,
            ],
            check=True,
        )

    @task(task_display_name="Normalize GeoJSON")
    def normalize_geojson(geojson_path: str) -> None:
        """Normalize GeoJSON FeatureCollection to a single Feature."""
        from workflows.utils.normalize_single_feature import normalize_geojson as normalize_to_single_feature

        normalize_to_single_feature(geojson_path)

    @task(task_display_name="Upload File")
    def upload_file(
        slug: str,
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
            destination_filename=f"{slug}-latest.boundary.{ext}",
            destination_path=f"boundaries/continents/{slug}/{subfolder}",
            destination_version="1",
            entity=slug,
            extension=ext,
            media_type=media_type,
            source=source,
            tags=CONTINENT_TAGS + [slug, subfolder],
        )

    @task(task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    dirs = prepare_directories()
    coastline_dl = download_coastline()
    cookie_dl = download_cookie_cutter()
    dirs >> [coastline_dl, cookie_dl]

    upload_tasks = []

    for continent in CONTINENTS:
        slug = continent["slug"]
        db_key = slug.replace("-", "_")

        tmp_dir = f"{WORK_DIR}/{slug}"
        clipped_path = f"{tmp_dir}/clipped.gpkg"
        dissolved_path = f"{tmp_dir}/dissolved.gpkg"
        output_basename = f"{WORK_DIR}/output/{slug}-latest.boundary"
        output_gpkg = f"{output_basename}.gpkg"
        output_geojson = f"{output_basename}.geojson"
        output_parquet = f"{output_basename}.parquet"

        with TaskGroup(group_id=slug, group_display_name=f"Extract {continent['name']} Continent"):
            clip = Ogr2OgrOperator(
                task_id="clip_coastline",
                task_display_name="Clip Coastline",
                args=[
                    "-f", "GPKG", clipped_path,
                    COASTLINE_PATH, "planet_coastline",
                    "-where", "feature_class = 'land'",
                    "-clipsrc", COOKIE_CUTTER_PATH,
                    "-clipsrclayer", "continent_cutter",
                    "-clipsrcwhere", f"continent = '{db_key}'",
                    "-nln", "clipped",
                ],
            )

            dissolve = Ogr2OgrOperator(
                task_id="dissolve_polygons",
                task_display_name="Dissolve Polygons",
                args=[
                    "-f", "GPKG", dissolved_path, clipped_path,
                    "-dialect", "sqlite",
                    "-sql", f"SELECT ST_Union(geom) AS geom, '{db_key}' AS continent FROM clipped",
                    "-nln", "dissolved",
                ],
            )

            area_gpkg = compute_area_and_export_gpkg(
                slug, db_key, dissolved_path, output_gpkg,
            )

            export_geojson_op = Ogr2OgrOperator(
                task_id="export_geojson",
                task_display_name="Export GeoJSON",
                environment={"OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
                args=[
                    "-f", "GeoJSON", output_geojson,
                    output_gpkg, slug,
                    "-nln", slug,
                ],
            )

            normalize = normalize_geojson(output_geojson)

            export_parquet_op = Ogr2OgrOperator(
                task_id="export_parquet",
                task_display_name="Export GeoParquet",
                args=[
                    "-f", "Parquet", output_parquet,
                    output_gpkg, slug,
                    "-nln", slug,
                ],
            )

            upload_gpkg = upload_file(slug, output_gpkg, "gpkg", "application/geopackage+sqlite3", "geopackage")
            upload_geojson = upload_file(slug, output_geojson, "geojson", "application/geo+json", "geojson")
            upload_parquet = upload_file(slug, output_parquet, "parquet", "application/vnd.apache.parquet", "geoparquet")

            # Dependencies
            [coastline_dl, cookie_dl] >> clip >> dissolve >> area_gpkg
            area_gpkg >> [export_geojson_op, export_parquet_op, upload_gpkg]
            export_geojson_op >> normalize >> upload_geojson
            export_parquet_op >> upload_parquet

            upload_tasks += [upload_gpkg, upload_geojson, upload_parquet]

    upload_tasks >> cleanup()
