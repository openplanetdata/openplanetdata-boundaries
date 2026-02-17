"""
Region Boundary DAG - Monthly ISO 3166-2 region boundary extraction.

Schedule: Monthly 1st at 06:00 UTC
Consumes: planet GOL and coastline GPKG from R2

Pipeline:
1. Extract all ISO 3166-2 region boundaries from OSM via gol query
2. Split into individual .osm.geojson files per region code, grouped into batches
3. For each batch of BATCH_SIZE regions (BATCH_WORKERS processed in parallel):
   a. Clip coastline land polygons to region boundary (Docker ogr2ogr)
   b. Dissolve clipped polygons into single geometry (Docker ogr2ogr)
   c. Export GeoPackage with geodesic area via ST_Area (Docker ogr2ogr)
   d. Export GeoJSON and GeoParquet in parallel (Docker ogr2ogr)
   e. Upload all formats to R2 in parallel

Throughput: max_active_tasks(24) × BATCH_WORKERS(2) = 48 concurrent regions (memory-safe for 128Gi node)
"""

from __future__ import annotations

import json
import os
import shlex
import shutil
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from airflow.exceptions import AirflowException
from airflow.sdk import DAG, task

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem
from openplanetdata.airflow.defaults import (
    DOCKER_MOUNT,
    OPENPLANETDATA_WORK_DIR,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
    SHARED_PLANET_COASTLINE_GPKG_PATH,
    SHARED_PLANET_OSM_GOL_PATH,
)
from openplanetdata.airflow.operators.gol import DOCKER_USER, GolOperator

REGION_TAGS = ["boundaries", "regions", "openplanetdata"]

WORK_DIR = f"{OPENPLANETDATA_WORK_DIR}/boundaries/regions"
OPENSTREETMAP_REGIONS_GEOJSON = f"{WORK_DIR}/openstreetmap-regions.geojson"
GDAL_IMAGE = "ghcr.io/osgeo/gdal:ubuntu-full-latest"

# Batching: BATCH_SIZE regions per Airflow task, BATCH_WORKERS processed in parallel within each batch.
# Total concurrent regions = max_active_tasks × BATCH_WORKERS.
# Coastline GPKG is ~3GB; each ogr2ogr clip loads it into memory (~5-10GB/process).
# Keep max_active_tasks × BATCH_WORKERS ≤ 48 to avoid OOM on a 128Gi node.
BATCH_SIZE = 32
BATCH_WORKERS = 2


def _run_ogr2ogr(args: list[str], env: dict | None = None) -> None:
    """Run ogr2ogr inside the GDAL Docker container."""
    import docker
    from docker.types import Mount

    cmd = shlex.join(["ogr2ogr", *args])
    docker.from_env().containers.run(
        image=GDAL_IMAGE,
        command=f"bash -c {shlex.quote(cmd)}",
        environment=env or {},
        mounts=[Mount(**DOCKER_MOUNT)],
        remove=True,
        stderr=True,
        stdout=True,
        user=DOCKER_USER,
    )


def _run_region_pipeline(code: str) -> str | None:
    """Run clip → dissolve → export pipeline for one region (no upload).

    Safe to call from threads. Returns code on failure, None on success.
    """
    region_dir = f"{WORK_DIR}/{code}"

    # Skip already-processed regions so batch retries are idempotent.
    # Check all three outputs — a previous crash may have left only the .gpkg.
    if all(
        os.path.exists(f"{region_dir}/{code}-latest.boundary.{ext}")
        for ext in ("gpkg", "geojson", "parquet")
    ):
        print(f"[{code}] Already processed, skipping")
        return None

    try:
        os.makedirs(region_dir, exist_ok=True)

        print(f"[{code}] clip")
        _run_ogr2ogr([
            "-f", "GPKG", f"{region_dir}/clipped.gpkg",
            SHARED_PLANET_COASTLINE_GPKG_PATH, "land_polygons",
            "-clipsrc", f"{WORK_DIR}/{code}.osm.geojson",
            "-makevalid",
            "-nln", "clipped",
        ])

        print(f"[{code}] dissolve")
        _run_ogr2ogr([
            "-f", "GPKG", f"{region_dir}/dissolved.gpkg",
            f"{region_dir}/clipped.gpkg",
            "-dialect", "sqlite",
            "-sql", "SELECT ST_Union(geom) AS geom FROM clipped",
            "-nln", "dissolved",
        ])

        print(f"[{code}] export gpkg")
        _run_ogr2ogr([
            "-f", "GPKG", f"{region_dir}/{code}-latest.boundary.gpkg",
            f"{region_dir}/dissolved.gpkg",
            "-dialect", "sqlite",
            "-sql", f"""SELECT geom, '{code}' AS "ISO3166-2", ROUND(ST_Area(ST_Transform(geom, 6933)) / 1000000.0, 2) AS area FROM dissolved""",
            "-nln", code,
        ])

        print(f"[{code}] export geojson+parquet")
        # Export GeoJSON and GeoParquet in parallel.
        with ThreadPoolExecutor(max_workers=2) as ex:
            f_geojson = ex.submit(_run_ogr2ogr, [
                "-f", "GeoJSON", f"{region_dir}/{code}-latest.boundary.geojson",
                f"{region_dir}/{code}-latest.boundary.gpkg", code,
                "-nln", code,
            ], {"OGR_GEOJSON_MAX_OBJ_SIZE": "0"})
            f_parquet = ex.submit(_run_ogr2ogr, [
                "-f", "Parquet", f"{region_dir}/{code}-latest.boundary.parquet",
                f"{region_dir}/{code}-latest.boundary.gpkg", code,
                "-nln", code,
            ])
            f_geojson.result()
            f_parquet.result()

        return None

    except Exception as e:
        from docker.errors import ContainerError
        if isinstance(e, ContainerError):
            stderr = e.stderr.decode() if isinstance(e.stderr, bytes) else (e.stderr or "")
            print(f"[{code}] Processing failed (exit {e.exit_status}):\n{stderr.strip()}")
        else:
            print(f"[{code}] Processing failed: {e}")
        return code


def _upload_region_files(code: str, hook: R2IndexHook) -> str | None:
    """Upload all output files for one region using a pre-created hook.

    Must be called from the main Airflow task thread where the connection
    context is available. Returns code on failure, None on success.
    """
    iso_code = code.replace("-", ":", 1)
    region_dir = f"{WORK_DIR}/{code}"
    try:
        for ext, subfolder, media_type in [
            ("gpkg",    "geopackage", "application/geopackage+sqlite3"),
            ("geojson", "geojson",    "application/geo+json"),
            ("parquet", "geoparquet", "application/vnd.apache.parquet"),
        ]:
            hook.upload(
                bucket=R2_BUCKET,
                category="boundary",
                destination_filename=f"{code}-latest.boundary.{ext}",
                destination_path=f"boundaries/regions/{code}/{subfolder}",
                destination_version="v1",
                entity=iso_code,
                extension=ext,
                media_type=media_type,
                source=f"{region_dir}/{code}-latest.boundary.{ext}",
                tags=REGION_TAGS + [code, subfolder],
            )
        return None
    except Exception as e:
        print(f"[{code}] Upload failed: {e}")
        return code


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
    max_active_tasks=24,  # 24 batches × BATCH_WORKERS=2 → 48 concurrent regions (memory-safe)
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

    @task(task_display_name="Split Regions into Batches")
    def split_osm_region_boundaries_file_per_region_code() -> list[list[str]]:
        """Split raw GeoJSON into individual .osm.geojson files, returns codes grouped into batches."""
        region_features: dict[str, list] = {}

        with open(OPENSTREETMAP_REGIONS_GEOJSON, "r", encoding="utf-8") as fh:
            data = json.load(fh)

        if data.get("type") == "FeatureCollection":
            for feature in data.get("features", []):
                iso_code = feature.get("properties", {}).get("ISO3166-2")
                if not iso_code:
                    continue
                osm_region_code = iso_code.replace(":", "-")
                region_features.setdefault(osm_region_code, []).append(feature)

        def write_region_file(item: tuple) -> None:
            osm_region_code, features = item
            with open(f"{WORK_DIR}/{osm_region_code}.osm.geojson", "w", encoding="utf-8") as fh:
                json.dump({"type": "FeatureCollection", "features": features}, fh)

        with ThreadPoolExecutor() as executor:
            executor.map(write_region_file, region_features.items())

        all_codes = sorted(region_features.keys())
        return [all_codes[i:i + BATCH_SIZE] for i in range(0, len(all_codes), BATCH_SIZE)]

    @task(task_display_name="Process Batch", retries=1)
    def process_batch(codes: list[str]) -> None:
        """Process a batch: ogr2ogr pipeline in parallel, then upload from main thread."""
        # Step 1: Run ogr2ogr pipeline in parallel threads (Docker SDK is thread-safe).
        with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as executor:
            pipeline_results = list(executor.map(_run_region_pipeline, codes))

        pipeline_failed = {code for code, r in zip(codes, pipeline_results) if r is not None}

        # Step 2: Upload from the main task thread (R2IndexHook requires Airflow context).
        hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)
        upload_failed = set()
        for code in codes:
            if code not in pipeline_failed:
                if _upload_region_files(code, hook) is not None:
                    upload_failed.add(code)

        all_failed = pipeline_failed | upload_failed
        if all_failed:
            print(f"Batch failures ({len(all_failed)}/{len(codes)}):")
            for code in sorted(all_failed):
                print(f"  {code}")
            raise AirflowException(f"{len(all_failed)} region(s) failed: {sorted(all_failed)}")

    @task(task_display_name="Report Failures", trigger_rule="all_done")
    def report_failures() -> None:
        """Report regions that failed to produce output files."""
        all_codes = sorted(f[:-12] for f in os.listdir(WORK_DIR) if f.endswith(".osm.geojson"))
        failed = [
            code for code in all_codes
            if not os.path.exists(f"{WORK_DIR}/{code}/{code}-latest.boundary.gpkg")
        ]
        if failed:
            print(f"Processing failed for {len(failed)}/{len(all_codes)} region(s):")
            for code in failed:
                print(f"  {code}")
        else:
            print(f"All {len(all_codes)} regions processed successfully.")

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

    batches = split_osm_region_boundaries_file_per_region_code()
    extract_region_boundaries_from_osm >> batches

    process_groups = process_batch.expand(codes=batches)
    coastline_dl >> process_groups

    report = report_failures()
    process_groups >> report >> done()
    process_groups >> cleanup()
