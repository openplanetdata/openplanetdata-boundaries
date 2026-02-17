"""
Country Boundary DAG - Monthly country boundary extraction.

Schedule: Monthly 1st at 04:00 UTC
Consumes: coastline GPKG and planet GOL from R2

Per-country pipeline:
1. Extract boundary from OSM via gol query
2. Clip with coastline land polygons (Docker ogr2ogr)
3. Dissolve clipped polygons into single geometry (Docker ogr2ogr)
4. Export GeoPackage with geodesic area via ST_Area (Docker ogr2ogr)
5. Export GeoJSON and GeoParquet in parallel (Docker ogr2ogr)
6. Normalize GeoJSON to single Feature
7. Upload all formats to R2
"""

from __future__ import annotations

import os
import shutil
from datetime import timedelta

from airflow.sdk import DAG, TaskGroup, task

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.data.countries import COUNTRIES
from openplanetdata.airflow.defaults import OPENPLANETDATA_WORK_DIR, R2_BUCKET, R2INDEX_CONNECTION_ID, SHARED_PLANET_COASTLINE_GPKG_PATH, SHARED_PLANET_OSM_GOL_PATH
from openplanetdata.airflow.operators.gol import GolOperator
from openplanetdata.airflow.operators.ogr2ogr import Ogr2OgrOperator

COUNTRY_TAGS = ["boundaries", "countries", "openplanetdata"]

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/countries"


with DAG(
    catchup=False,
    dag_display_name="OpenPlanetData Boundaries Countries",
    dag_id="openplanetdata_boundaries_countries",
    default_args={
        "execution_timeout": timedelta(hours=1),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
    },
    description="Monthly country boundary extraction from OSM",
    doc_md=__doc__,
    max_active_runs=1,
    max_active_tasks=32,
    schedule="0 4 1 * *",
    tags=["boundaries", "countries", "openplanetdata"],
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
            destination=SHARED_PLANET_COASTLINE_GPKG_PATH,
            source_filename="planet-latest.coastline.gpkg",
            source_path="boundaries/coastline/geopackage",
            source_version="v1",
        )

    @task(task_display_name="Prepare Directories")
    def prepare_directories() -> None:
        """Create working directories for all countries."""
        for code in sorted(COUNTRIES.keys()):
            os.makedirs(f"{WORK_DIR}/{code}", exist_ok=True)

    @task(task_display_name="Normalize GeoJSON")
    def normalize_geojson(geojson_path: str) -> None:
        """Normalize GeoJSON FeatureCollection to a single Feature."""
        import json

        with open(geojson_path, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        if data.get("type") == "FeatureCollection":
            features = data.get("features") or []
            if not features:
                raise ValueError("No features found in FeatureCollection")
            feature = features[0]
        elif data.get("type") == "Feature":
            feature = data
        else:
            raise ValueError(f"Unsupported GeoJSON type: {data.get('type')}")

        with open(geojson_path, "w", encoding="utf-8") as fh:
            json.dump(feature, fh, ensure_ascii=False, separators=(",", ":"))

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
            destination_path=f"boundaries/countries/{slug}/{subfolder}",
            destination_version="v1",
            entity=slug,
            extension=ext,
            media_type=media_type,
            source=source,
            tags=COUNTRY_TAGS + [slug, subfolder],
        )

    @task(task_display_name="Done")
    def done() -> None:
        """No-op gate task to propagate upstream failures to DAG run state."""

    @task(task_display_name="Cleanup", trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    dirs = prepare_directories()
    gol_dl = download_planet_gol()
    coastline_dl = download_coastline()
    dirs >> [gol_dl, coastline_dl]

    upload_tasks = []

    for code in sorted(COUNTRIES.keys()):
        country = COUNTRIES[code]
        slug = code
        name = country["name"]
        safe_name = name.replace("'", "''")

        tmp_dir = f"{WORK_DIR}/{slug}"
        raw_geojson = f"{tmp_dir}/raw.geojson"
        clipped_path = f"{tmp_dir}/clipped.gpkg"
        dissolved_path = f"{tmp_dir}/dissolved.gpkg"
        output_basename = f"{tmp_dir}/{slug}-latest.boundary"
        output_gpkg = f"{output_basename}.gpkg"
        output_geojson = f"{output_basename}.geojson"
        output_parquet = f"{output_basename}.parquet"

        with TaskGroup(group_id=slug, group_display_name=f"Extract {name}"):
            extract = GolOperator(
                task_id="extract_boundary",
                task_display_name="Extract Boundary",
                args=["query", SHARED_PLANET_OSM_GOL_PATH, f'a["ISO3166-1:alpha2"="{code}"]', "-f", "geojson"],
                output_file=raw_geojson,
            )

            clip = Ogr2OgrOperator(
                task_id="clip_coastline",
                task_display_name="Clip Coastline",
                args=[
                    "-f", "GPKG", clipped_path,
                    SHARED_PLANET_COASTLINE_GPKG_PATH, "land_polygons",
                    "-clipsrc", raw_geojson,
                    "-nln", "clipped",
                ],
            )

            dissolve = Ogr2OgrOperator(
                task_id="dissolve_polygons",
                task_display_name="Dissolve Polygons",
                args=[
                    "-f", "GPKG", dissolved_path, clipped_path,
                    "-dialect", "sqlite",
                    "-sql", "SELECT ST_Union(geom) AS geom FROM clipped",
                    "-nln", "dissolved",
                ],
            )

            export_gpkg = Ogr2OgrOperator(
                task_id="export_gpkg",
                task_display_name="Export GeoPackage",
                args=[
                    "-f", "GPKG", output_gpkg, dissolved_path,
                    "-dialect", "sqlite",
                    "-sql", f"""SELECT geom, '{code}' AS "ISO3166-1:alpha2", '{safe_name}' AS name, ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved""",
                    "-nln", slug,
                ],
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

            upload_gpkg = upload_file.override(task_display_name="Upload GeoPackage")(slug, output_gpkg, "gpkg", "application/geopackage+sqlite3", "geopackage")
            upload_geojson = upload_file.override(task_display_name="Upload GeoJSON")(slug, output_geojson, "geojson", "application/geo+json", "geojson")
            upload_parquet = upload_file.override(task_display_name="Upload GeoParquet")(slug, output_parquet, "parquet", "application/vnd.apache.parquet", "geoparquet")

            # Dependencies
            gol_dl >> extract
            [extract, coastline_dl] >> clip >> dissolve >> export_gpkg
            export_gpkg >> [export_geojson_op, export_parquet_op, upload_gpkg]
            export_geojson_op >> normalize >> upload_geojson
            export_parquet_op >> upload_parquet

            upload_tasks += [upload_gpkg, upload_geojson, upload_parquet]

    upload_tasks >> done()
    upload_tasks >> cleanup()
