"""
Planet Coastline DAG - Daily coastline generation from OSM data.

Schedule: Daily at 14:00 UTC
Produces Asset: coastline_gpkg (triggers downstream DAGs)
"""

import shutil
from datetime import timedelta
from pathlib import Path

from airflow.exceptions import AirflowException
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, task
from docker.types import Mount
from elaunira.airflow.providers.r2index.operators import DownloadItem, UploadItem
from elaunira.r2index.storage import R2TransferConfig
from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    OPENPLANETDATA_IMAGE,
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_OSM_PBF_PATH,
)
from workflows.utils.osmcoastline_report import main as parse_osmcoastline_log

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/coastline"

COASTLINE_GPKG_PATH = f"{WORK_DIR}/coastline.gpkg"
COASTLINE_GPKG_COPY_PATH = f"{WORK_DIR}/coastline-copy.gpkg"
COASTLINE_GEOJSON_PATH = f"{WORK_DIR}/coastline.geojson"
COASTLINE_PARQUET_PATH = f"{WORK_DIR}/coastline.parquet"
OSMCOASTLINE_EXIT_CODE_PATH = f"{WORK_DIR}/osmcoastline.exitcode"
OSMCOASTLINE_LOG_PATH = f"{WORK_DIR}/osmcoastline.log"

SQL = (
    "SELECT 'land' AS feature_class, a.* FROM land_polygons AS a "
    "UNION ALL "
    "SELECT 'water' AS feature_class, b.* FROM water_polygons AS b"
)

with DAG(
    dag_id="openplanetdata-boundaries-coastline",
    default_args={
        "execution_timeout": timedelta(hours=2),
        "executor": "airflow.providers.edge3.executors.EdgeExecutor",
        "owner": "openplanetdata",
        "queue": "cortex"
    },
    description="Daily planet coastline extraction from OSM planet PBF",
    doc_md=__doc__,
    max_active_runs=1,
    schedule="0 14 * * *",
    tags=["openplanetdata", "osm", "coastline"],
) as dag:

    @task.r2index_download(
        task_id="download-planet-pbf",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        transfer_config=R2TransferConfig(max_concurrency=64, multipart_chunksize=32 * 1024 * 1024),
    )
    def download_planet_pbf() -> DownloadItem:
        """Download planet PBF from R2."""
        return DownloadItem(
            destination=SHARED_PLANET_OSM_PBF_PATH,
            overwrite=False,
            source_filename="planet-latest.osm.pbf",
            source_path="osm/planet/pbf",
            source_version="v1",
        )

    run_osmcoastline = DockerOperator(
        task_id="run-osmcoastline",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            mkdir -p {WORK_DIR} &&
            osmcoastline {SHARED_PLANET_OSM_PBF_PATH} \
                -o {COASTLINE_GPKG_PATH} -g GPKG -p both -v -f \
                2>&1 | tee {OSMCOASTLINE_LOG_PATH};
            rc=${{PIPESTATUS[0]}};
            echo "$rc" > {OSMCOASTLINE_EXIT_CODE_PATH};
            echo "osmcoastline completed with exit code $rc";
            ls -lh {WORK_DIR}
        '""",
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task(task_id="parse-osmcoastline-logs", retries=0)
    def parse_osmcoastline_logs() -> None:
        """Parse osmcoastline logs and print report. Fails if exit code > 2."""
        parse_osmcoastline_log(OSMCOASTLINE_LOG_PATH, COASTLINE_GPKG_PATH)
        exit_code = int(Path(OSMCOASTLINE_EXIT_CODE_PATH).read_text().strip())
        if exit_code > 2:
            raise AirflowException(f"osmcoastline failed with exit code {exit_code}")

    @task(task_id="copy-gpkg")
    def copy_gpkg() -> None:
        """Copy GPKG so exports can read in parallel without SQLite locks."""
        shutil.copy2(COASTLINE_GPKG_PATH, COASTLINE_GPKG_COPY_PATH)

    export_geojson = DockerOperator(
        task_id="export-geojson",
        image="ghcr.io/osgeo/gdal:ubuntu-small-latest",
        command=f"""bash -c "
            ogr2ogr -f GeoJSON {COASTLINE_GEOJSON_PATH} {COASTLINE_GPKG_PATH} \
                -dialect SQLite \
                -sql \\"{SQL}\\" \
                -nln planet_coastline -lco RFC7946=YES -lco COORDINATE_PRECISION=6
        " """,
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    export_parquet = DockerOperator(
        task_id="export-parquet",
        image="ghcr.io/osgeo/gdal:ubuntu-full-latest",
        command=f"""bash -c "
            ogr2ogr -f Parquet {COASTLINE_PARQUET_PATH} {COASTLINE_GPKG_COPY_PATH} \
                -dialect SQLite \
                -sql \\"{SQL}\\" \
                -nln planet_coastline -lco COMPRESSION=ZSTD
        " """,
        force_pull=True,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    UPLOAD_BASE_PATH = "boundaries/coastline"
    UPLOAD_FILENAME_BASE = "planet-latest.coastline"
    UPLOAD_TAGS = ["coastline", "openstreetmap", "private"]

    @task.r2index_upload(
        task_id="upload-gpkg",
        bucket=R2_BUCKET,
        outlets=[Asset(
            name="openplanetdata-coastline-gpkg",
            uri=f"s3://{R2_BUCKET}/boundaries/coastline/geopackage/v1/planet-latest.coastline.gpkg",
        )],
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_gpkg() -> list[UploadItem]:
        """Upload GeoPackage to R2."""
        return [UploadItem(
            category="coastline",
            destination_filename=f"{UPLOAD_FILENAME_BASE}.gpkg",
            destination_path=f"{UPLOAD_BASE_PATH}/geopackage",
            destination_version="v1",
            entity="planet",
            extension="gpkg",
            media_type="application/geopackage+sqlite3",
            source=COASTLINE_GPKG_PATH,
            tags=UPLOAD_TAGS + ["geopackage"],
        )]

    @task.r2index_upload(
        task_id="upload-geojson",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_geojson() -> list[UploadItem]:
        """Upload GeoJSON to R2."""
        return [UploadItem(
            category="coastline",
            destination_filename=f"{UPLOAD_FILENAME_BASE}.geojson",
            destination_path=f"{UPLOAD_BASE_PATH}/geojson",
            destination_version="v1",
            entity="planet",
            extension="geojson",
            media_type="application/geo+json",
            source=COASTLINE_GEOJSON_PATH,
            tags=UPLOAD_TAGS + ["geojson"],
        )]

    @task.r2index_upload(
        task_id="upload-geoparquet",
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
    )
    def upload_geoparquet() -> list[UploadItem]:
        """Upload GeoParquet to R2."""
        return [UploadItem(
            category="coastline",
            destination_filename=f"{UPLOAD_FILENAME_BASE}.parquet",
            destination_path=f"{UPLOAD_BASE_PATH}/geoparquet",
            destination_version="v1",
            entity="planet",
            extension="parquet",
            media_type="application/vnd.apache.parquet",
            source=COASTLINE_PARQUET_PATH,
            tags=UPLOAD_TAGS + ["geoparquet"],
        )]

    @task(trigger_rule="all_done")
    def cleanup() -> None:
        """Clean up working directory."""
        shutil.rmtree(WORK_DIR, ignore_errors=True)

    # Task flow
    download_result = download_planet_pbf()
    download_result >> run_osmcoastline
    osmcoastline_logs_parse = parse_osmcoastline_logs()
    run_osmcoastline >> osmcoastline_logs_parse

    gpkg_upload = upload_gpkg()
    geojson_upload = upload_geojson()
    geoparquet_upload = upload_geoparquet()

    osmcoastline_logs_parse >> gpkg_upload
    osmcoastline_logs_parse >> export_geojson >> geojson_upload
    gpkg_copy = copy_gpkg()
    osmcoastline_logs_parse >> gpkg_copy >> export_parquet >> geoparquet_upload

    [gpkg_upload, geojson_upload, geoparquet_upload] >> cleanup()
