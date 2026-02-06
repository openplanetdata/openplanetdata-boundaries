"""
Planet Coastline DAG - Daily coastline generation from OSM data.

Schedule: Daily at 14:00 UTC
Produces Asset: coastline_gpkg (triggers downstream DAGs)
"""

import os
import shutil
from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task
from elaunira.airflow.providers.r2index.operators import DownloadItem, UploadItem

from workflows.config import R2_BUCKET, R2_CONN_ID
from workflows.utils.osmcoastline_report import main as parse_osmcoastline_log

coastline_gpkg = Asset(
    name="openplanetdata-coastline-gpkg",
    uri=f"s3://{R2_BUCKET}/boundaries/coastline/geopackage/latest/planet-latest.coastline.gpkg",
)

WORK_DIR = "/data/openplanetdata/coastline"
PBF_PATH = f"{WORK_DIR}/planet-latest.osm.pbf"
GPKG_PATH = f"{WORK_DIR}/coastline.gpkg"
LOG_PATH = f"{WORK_DIR}/osmcoastline.log"
GEOJSON_PATH = f"{WORK_DIR}/coastline.geojson"
PARQUET_PATH = f"{WORK_DIR}/coastline.parquet"
SQL = "SELECT 'land' AS feature_class, a.* FROM land_polygons AS a UNION ALL SELECT 'water' AS feature_class, b.* FROM water_polygons AS b"

with DAG(
    dag_id="openplanetdata-planet-coastline",
    default_args={
        "execution_timeout": timedelta(hours=2),
        "owner": "openplanetdata",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Daily planet coastline extraction from OSM planet PBF",
    doc_md=__doc__,
    schedule="0 14 * * *",
    tags=["openplanetdata", "osm", "coastline"],
) as dag:

    @task.r2index_download(bucket=R2_BUCKET, r2index_conn_id=R2_CONN_ID)
    def download_planet_pbf() -> DownloadItem:
        """Download planet PBF from R2."""
        return DownloadItem(
            destination=f"{WORK_DIR}/shared/planet-latest.osm.pbf",
            source_filename="planet-latest.osm.pbf",
            source_path="osm/planet/pbf",
            source_version="1",
        )

    @task.bash
    def run_osmcoastline(download_result: list[dict]) -> str:
        """Extract coastline from PBF using osmcoastline."""
        pbf_path = download_result[0]["path"]
        return f"""
            set -euo pipefail
            echo "Running osmcoastline on {pbf_path}"
            osmcoastline "{pbf_path}" -o {GPKG_PATH} -g GPKG -p both -v -f -e 2>&1 | tee {WORK_DIR}/osmcoastline.log
            EXIT_CODE=${{PIPESTATUS[0]}}
            if [ $EXIT_CODE -gt 2 ]; then
                echo "osmcoastline failed with exit code $EXIT_CODE"
                exit $EXIT_CODE
            fi
            echo "osmcoastline completed with exit code $EXIT_CODE"
        """

    @task
    def parse_osmcoastline_logs() -> None:
        """Parse osmcoastline logs and print report."""
        parse_osmcoastline_log(LOG_PATH)

    @task.bash
    def export_geojson() -> str:
        """Convert GeoPackage to GeoJSON."""
        return f"""
            set -euo pipefail
            export OGR_GEOJSON_MAX_OBJ_SIZE=0
            echo "Exporting to GeoJSON..."
            ogr2ogr -f GeoJSON {GEOJSON_PATH} {GPKG_PATH} \
                -dialect SQLite -sql "{SQL}" \
                -nln planet_coastline -lco RFC7946=YES -lco COORDINATE_PRECISION=6
        """

    @task.bash
    def export_parquet() -> str:
        """Convert GeoPackage to GeoParquet."""
        return f"""
            set -euo pipefail
            echo "Exporting to GeoParquet..."
            ogr2ogr -f Parquet {PARQUET_PATH} {GPKG_PATH} \
                -dialect SQLite -sql "{SQL}" \
                -nln planet_coastline -lco COMPRESSION=ZSTD
        """

    @task.r2index_upload(bucket=R2_BUCKET, r2index_conn_id=R2_CONN_ID, outlets=[coastline_gpkg])
    def upload() -> list[UploadItem]:
        """Upload all boundary formats to R2."""
        base_path = "boundaries/coastline"
        filename_base = "planet-latest.coastline"
        tags = ["coastline", "openstreetmap", "private"]

        return [
            UploadItem(
                category="coastline",
                destination_filename=f"{filename_base}.geojson",
                destination_path=f"{base_path}/geojson",
                destination_version="1",
                entity="planet",
                extension="geojson",
                media_type="application/geo+json",
                source=GEOJSON_PATH,
                tags=tags + ["geojson"],
            ),
            UploadItem(
                category="coastline",
                destination_filename=f"{filename_base}.gpkg",
                destination_path=f"{base_path}/geopackage",
                destination_version="1",
                entity="planet",
                extension="gpkg",
                media_type="application/geopackage+sqlite3",
                source=GPKG_PATH,
                tags=tags + ["geopackage"],
            ),
            UploadItem(
                category="coastline",
                destination_filename=f"{filename_base}.parquet",
                destination_path=f"{base_path}/geoparquet",
                destination_version="1",
                entity="planet",
                extension="parquet",
                media_type="application/vnd.apache.parquet",
                source=PARQUET_PATH,
                tags=tags + ["geoparquet"],
            ),
        ]

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up temporary files."""
        if os.path.exists(WORK_DIR):
            shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_result = download_planet_pbf()
    osmcoastline_done = run_osmcoastline(download_result)
    parsed = parse_osmcoastline_logs()
    geojson = export_geojson()
    parquet = export_parquet()
    upload_result = upload()
    osmcoastline_done >> [parsed, geojson, parquet]
    [geojson, parquet] >> upload_result
    [parsed, upload_result] >> cleanup()
