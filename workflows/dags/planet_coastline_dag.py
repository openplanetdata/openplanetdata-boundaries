"""
Planet Coastline DAG - Daily coastline generation from OSM data.

Schedule: Daily at 14:00 UTC
Produces Asset: coastline_gpkg (triggers downstream DAGs)
"""

import time
from datetime import timedelta

from airflow.sdk import DAG, Asset, task
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount
from elaunira.airflow.providers.r2index.operators import DownloadItem, UploadItem
from elaunira.r2index.storage import R2TransferConfig

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
GEO_IMAGE = "openplanetdata/openplanetdata-airflow:latest"
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

    @task.r2index_download(
        bucket=R2_BUCKET,
        r2index_conn_id=R2_CONN_ID,
        transfer_config=R2TransferConfig(max_concurrency=64, multipart_chunksize=32 * 1024 * 1024),
    )
    def download_planet_pbf() -> DownloadItem:
        """Download planet PBF from R2."""
        return DownloadItem(
            destination=f"{WORK_DIR}/shared/planet-latest.osm.pbf",
            overwrite=False,
            source_filename="planet-latest.osm.pbf",
            source_path="osm/planet/pbf",
            source_version="v1",
        )

    run_osmcoastline_op = DockerOperator(
        task_id="run_osmcoastline",
        image=GEO_IMAGE,
        command=f"""bash -c '
            osmcoastline {WORK_DIR}/shared/planet-latest.osm.pbf \
                -o {GPKG_PATH} -g GPKG -p both -v -f \
                2>&1 | tee {LOG_PATH};
            rc=${{PIPESTATUS[0]}};
            echo "osmcoastline completed with exit code $rc";
            [ "$rc" -le 2 ]
        '""",
        mounts=[GEO_MOUNT],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task
    def parse_osmcoastline_logs() -> None:
        """Parse osmcoastline logs and print report."""
        parse_osmcoastline_log(LOG_PATH)

    export_geojson_op = DockerOperator(
        task_id="export_geojson",
        image=GEO_IMAGE,
        command=f"""bash -c "
            ogr2ogr -f GeoJSON {GEOJSON_PATH} {GPKG_PATH} \
                -dialect SQLite \
                -sql \\"{SQL}\\" \
                -nln planet_coastline -lco RFC7946=YES -lco COORDINATE_PRECISION=6
        " """,
        mounts=[GEO_MOUNT],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    export_parquet_op = DockerOperator(
        task_id="export_parquet",
        image=GEO_IMAGE,
        command=f"""bash -c "
            ogr2ogr -f Parquet {PARQUET_PATH} {GPKG_PATH} \
                -dialect SQLite \
                -sql \\"{SQL}\\" \
                -nln planet_coastline -lco COMPRESSION=ZSTD
        " """,
        mounts=[GEO_MOUNT],
        mount_tmp_dir=False,
        auto_remove="success",
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

    @task
    def test_live_logs() -> None:
        """Temporary task to verify live log streaming from edge worker."""
        for i in range(1, 31):
            print(f"[log test] tick {i}/30")
            time.sleep(2)
        print("[log test] done")

    # Task flow
    download_result = download_planet_pbf()
    download_result >> run_osmcoastline_op
    parsed = parse_osmcoastline_logs()
    upload_result = upload()
    run_osmcoastline_op >> [parsed, export_geojson_op, export_parquet_op]
    [export_geojson_op, export_parquet_op] >> upload_result

    # Standalone test task — trigger manually, remove after verification
    test_live_logs()
