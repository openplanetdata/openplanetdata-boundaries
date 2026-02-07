"""
Planet Coastline DAG - Daily coastline generation from OSM data.

Schedule: Daily at 14:00 UTC
Produces Asset: coastline_gpkg (triggers downstream DAGs)
"""

import os
import shutil
from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task
from docker.types import Mount
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

# Geospatial tools run inside this Docker image via docker.sock
GEO_IMAGE = "ghcr.io/openplanetdata/airflow:latest"
GEO_MOUNT = Mount(source="/data/airflow", target="/data", type="bind")

with DAG(
    dag_id="openplanetdata-planet-coastline",
    default_args={
        "execution_timeout": timedelta(hours=2),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
    description="Daily planet coastline extraction from OSM planet PBF",
    doc_md=__doc__,
    schedule="0 14 * * *",
    tags=["openplanetdata", "osm", "coastline"],
) as dag:

    @task
    def check_r2index_connection() -> None:
        """Check R2Index connection via OpenBao before running the pipeline."""
        from elaunira.airflow.providers.r2index.hooks.r2index import R2IndexHook

        hook = R2IndexHook(r2index_conn_id=R2_CONN_ID)
        config = hook._get_config_from_connection()

        if config is None:
            raise RuntimeError(f"Connection '{R2_CONN_ID}' returned no config")

        # Log resolved values (mask secrets)
        for key, value in config.items():
            if value is None:
                print(f"  {key}: NOT SET")
            elif "secret" in key or "token" in key:
                print(f"  {key}: {value[:8]}...{value[-4:]}" if len(value) > 12 else f"  {key}: ***")
            else:
                print(f"  {key}: {value}")

        missing = [k for k, v in config.items() if not v]
        if missing:
            raise RuntimeError(f"Missing config values: {missing}")

        # Quick API health check
        import urllib.request

        token = config["index_api_token"]
        print(f"  token length: {len(token)}, repr: {repr(token[:12])}...{repr(token[-4:])}")

        url = config["index_api_url"].rstrip("/") + "/files?limit=1"
        req = urllib.request.Request(
            url,
            headers={"Authorization": f"Bearer {token}"},
        )
        try:
            with urllib.request.urlopen(req) as resp:
                print(f"  API check: HTTP {resp.status}")
        except urllib.error.HTTPError as e:
            body = e.read().decode()
            raise RuntimeError(f"API check failed: HTTP {e.code} - {body}") from e

    @task.r2index_download(bucket=R2_BUCKET, r2index_conn_id=R2_CONN_ID)
    def download_planet_pbf() -> DownloadItem:
        """Download planet PBF from R2."""
        return DownloadItem(
            destination=f"{WORK_DIR}/shared/planet-latest.osm.pbf",
            source_filename="planet-latest.osm.pbf",
            source_path="osm/planet/pbf",
            source_version="v1",
        )

    @task.docker(image=GEO_IMAGE, mounts=[GEO_MOUNT], mount_tmp_dir=False, auto_remove="success")
    def run_osmcoastline(download_result: list[dict]) -> None:
        """Extract coastline from PBF using osmcoastline."""
        import subprocess
        import sys

        pbf_path = download_result[0]["path"]

        print(f"Running osmcoastline on {pbf_path}")
        proc = subprocess.Popen(
            ["osmcoastline", pbf_path, "-o", "/data/openplanetdata/coastline/coastline.gpkg",
             "-g", "GPKG", "-p", "both", "-v", "-f", "-e"],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )

        with open("/data/openplanetdata/coastline/osmcoastline.log", "wb") as f:
            for line in proc.stdout:
                sys.stdout.buffer.write(line)
                f.write(line)

        if proc.wait() > 2:
            raise RuntimeError(f"osmcoastline failed with exit code {proc.returncode}")
        print(f"osmcoastline completed with exit code {proc.returncode}")

    @task
    def parse_osmcoastline_logs() -> None:
        """Parse osmcoastline logs and print report."""
        parse_osmcoastline_log(LOG_PATH)

    @task.docker(image=GEO_IMAGE, mounts=[GEO_MOUNT], mount_tmp_dir=False, auto_remove="success")
    def export_geojson() -> None:
        """Convert GeoPackage to GeoJSON."""
        import subprocess

        subprocess.run(
            ["ogr2ogr", "-f", "GeoJSON",
             "/data/openplanetdata/coastline/coastline.geojson",
             "/data/openplanetdata/coastline/coastline.gpkg",
             "-dialect", "SQLite",
             "-sql", "SELECT 'land' AS feature_class, a.* FROM land_polygons AS a UNION ALL SELECT 'water' AS feature_class, b.* FROM water_polygons AS b",
             "-nln", "planet_coastline", "-lco", "RFC7946=YES", "-lco", "COORDINATE_PRECISION=6"],
            check=True,
        )

    @task.docker(image=GEO_IMAGE, mounts=[GEO_MOUNT], mount_tmp_dir=False, auto_remove="success")
    def export_parquet() -> None:
        """Convert GeoPackage to GeoParquet."""
        import subprocess

        subprocess.run(
            ["ogr2ogr", "-f", "Parquet",
             "/data/openplanetdata/coastline/coastline.parquet",
             "/data/openplanetdata/coastline/coastline.gpkg",
             "-dialect", "SQLite",
             "-sql", "SELECT 'land' AS feature_class, a.* FROM land_polygons AS a UNION ALL SELECT 'water' AS feature_class, b.* FROM water_polygons AS b",
             "-nln", "planet_coastline", "-lco", "COMPRESSION=ZSTD"],
            check=True,
        )

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
    conn_check = check_r2index_connection()
    download_result = download_planet_pbf()
    conn_check >> download_result
    osmcoastline_done = run_osmcoastline(download_result)
    parsed = parse_osmcoastline_logs()
    geojson = export_geojson()
    parquet = export_parquet()
    upload_result = upload()
    osmcoastline_done >> [parsed, geojson, parquet]
    [geojson, parquet] >> upload_result
    [parsed, upload_result] >> cleanup()
