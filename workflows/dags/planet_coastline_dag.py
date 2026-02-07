"""
Planet Coastline DAG - Daily coastline generation from OSM data.

Schedule: Daily at 14:00 UTC
Produces Asset: coastline_gpkg (triggers downstream DAGs)
"""

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, Asset, task
from datetime import timedelta
from docker.types import Mount
from elaunira.airflow.providers.r2index.operators import DownloadItem, UploadItem
from elaunira.r2index.storage import R2TransferConfig
from openplanetdata.airflow.defaults import DOCKER_MOUNT, OPENPLANETDATA_IMAGE, SHARED_PLANET_OSM_PBF_PATH

from workflows.config import R2_BUCKET, R2INDEX_CONNECTION_ID
from workflows.utils.osmcoastline_report import main as parse_osmcoastline_log

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/coastline"

COASTLINE_GPKG_PATH = f"{WORK_DIR}/coastline.gpkg"
COASTLINE_GEOJSON_PATH = f"{WORK_DIR}/coastline.geojson"
COASTLINE_PARQUET_PATH = f"{WORK_DIR}/coastline.parquet"

SQL = "SELECT 'land' AS feature_class, a.* FROM land_polygons AS a UNION ALL SELECT 'water' AS feature_class, b.* FROM water_polygons AS b"

WORK_DIR = "/data/openplanetdata"

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
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        transfer_config=R2TransferConfig(max_concurrency=64, multipart_chunksize=32 * 1024 * 1024),
    )
    def download_planet_pbf() -> DownloadItem:
        """Download planet PBF from R2."""
        return DownloadItem(
            destination=f"{SHARED_PLANET_OSM_PBF_PATH}",
            overwrite=False,
            source_filename="planet-latest.osm.pbf",
            source_path="osm/planet/pbf",
            source_version="v1",
        )


    OSMCOASTLINE_LOG_PATH = f"{WORK_DIR}/osmcoastline.log"

    run_osmcoastline_op = DockerOperator(
        task_id="run_osmcoastline",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c '
            osmcoastline {SHARED_PLANET_OSM_PBF_PATH} \
                -o {COASTLINE_GPKG_PATH} -g GPKG -p both -v -f \
                2>&1 | tee {OSMCOASTLINE_LOG_PATH};
            rc=${{PIPESTATUS[0]}};
            echo "osmcoastline completed with exit code $rc";
            [ "$rc" -le 2 ]
        '""",
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )


    @task
    def parse_osmcoastline_logs() -> None:
        """Parse osmcoastline logs and print report."""
        parse_osmcoastline_log(OSMCOASTLINE_LOG_PATH)


    export_geojson = DockerOperator(
        task_id="export_geojson",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c "
            ogr2ogr -f GeoJSON {COASTLINE_GEOJSON_PATH} {COASTLINE_GPKG_PATH} \
                -dialect SQLite \
                -sql \\"{SQL}\\" \
                -nln planet_coastline -lco RFC7946=YES -lco COORDINATE_PRECISION=6
        " """,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    export_parquet = DockerOperator(
        task_id="export_parquet",
        image=OPENPLANETDATA_IMAGE,
        command=f"""bash -c "
            ogr2ogr -f Parquet {COASTLINE_PARQUET_PATH} {COASTLINE_GPKG_PATH} \
                -dialect SQLite \
                -sql \\"{SQL}\\" \
                -nln planet_coastline -lco COMPRESSION=ZSTD
        " """,
        mounts=[Mount(**DOCKER_MOUNT)],
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task.r2index_upload(bucket=R2_BUCKET, outlets=[Asset(
        name="openplanetdata-coastline-gpkg",
        uri=f"s3://{R2_BUCKET}/boundaries/coastline/geopackage/v1/planet-latest.coastline.gpkg",
    )], r2index_conn_id=R2INDEX_CONNECTION_ID)
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
                destination_version="v1",
                entity="planet",
                extension="geojson",
                media_type="application/geo+json",
                source=COASTLINE_GEOJSON_PATH,
                tags=tags + ["geojson"],
            ),
            UploadItem(
                category="coastline",
                destination_filename=f"{filename_base}.gpkg",
                destination_path=f"{base_path}/geopackage",
                destination_version="v1",
                entity="planet",
                extension="gpkg",
                media_type="application/geopackage+sqlite3",
                source=COASTLINE_GPKG_PATH,
                tags=tags + ["geopackage"],
            ),
            UploadItem(
                category="coastline",
                destination_filename=f"{filename_base}.parquet",
                destination_path=f"{base_path}/geoparquet",
                destination_version="v1",
                entity="planet",
                extension="parquet",
                media_type="application/vnd.apache.parquet",
                source=COASTLINE_PARQUET_PATH,
                tags=tags + ["geoparquet"],
            ),
        ]


    # Task flow
    download_result = download_planet_pbf()
    download_result >> run_osmcoastline_op
    parsed = parse_osmcoastline_logs()
    upload_result = upload()
    run_osmcoastline_op >> parsed >> [export_geojson, export_parquet] >> upload_result
