"""
Continent Boundary DAG - Monthly continent boundary extraction.

Schedule: Monthly 1st at 05:00 UTC
Source: .github/workflows/boundary-continent.yaml
Consumes Asset: coastline_gpkg

Required tools (provided by WORKER_IMAGE):
- ogr2ogr, ogrinfo: Geometry operations and format conversions
- python3 with pyproj: Area calculations and metadata

Continents: africa, antarctica, asia, europe, north-america, oceania, south-america

Tasks:
1. download_coastline / download_cookie_cutter - Download shared resources
2. prepare_continents - Return list of continent dicts
3. extract_and_upload.expand() - Dynamic mapping for parallel continent processing
4. cleanup - Remove temporary files
"""

from __future__ import annotations

import os
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, Asset, task

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem

from workflows.config import (
    COASTLINE_GPKG_REF,
    CONTINENT_COOKIE_CUTTER_REF,
    CONTINENT_TAGS,
    MAX_PARALLEL_CONTINENTS,
    POD_CONFIG_CONTINENT_EXTRACTION,
    POD_CONFIG_DEFAULT,
    R2_BUCKET,
    R2INDEX_CONNECTION_ID,
)
from workflows.data.continents import CONTINENTS

# Reference to coastline asset (consumed by this DAG)
coastline_gpkg = Asset(
    name="coastline_gpkg",
    uri=f"s3://{R2_BUCKET}/boundaries/coastline/geopackage/latest/planet-latest.coastline.gpkg",
)

WORK_DIR = "/data/openplanetdata/continent_boundaries"


with DAG(
    catchup=False,
    dag_id="boundary_continent",
    default_args={
        "depends_on_past": False,
        "execution_timeout": timedelta(hours=2),
        "owner": "openplanetdata",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    description="Monthly continent boundary extraction from OSM coastline",
    doc_md=__doc__,
    max_active_tasks=MAX_PARALLEL_CONTINENTS,
    schedule="0 5 1 * *",
    start_date=datetime(2024, 1, 1),
    tags=["boundary", "continent", "osm", "monthly"],
) as dag:

    @task(executor_config=POD_CONFIG_DEFAULT)
    def get_date_tag() -> str:
        """Generate date tag for file naming."""
        return datetime.utcnow().strftime("%Y%m%d")

    @task.r2index_download(
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        executor_config=POD_CONFIG_DEFAULT,
    )
    def download_coastline() -> DownloadItem:
        """Download coastline GPKG from R2."""
        coast_path, coast_filename, coast_version = COASTLINE_GPKG_REF
        return DownloadItem(
            destination=os.path.join(WORK_DIR, "planet-latest.coastline.gpkg"),
            source_filename=coast_filename,
            source_path=coast_path,
            source_version=coast_version,
        )

    @task.r2index_download(
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        executor_config=POD_CONFIG_DEFAULT,
    )
    def download_cookie_cutter() -> DownloadItem:
        """Download continent cookie-cutter GPKG from R2."""
        cc_path, cc_filename, cc_version = CONTINENT_COOKIE_CUTTER_REF
        return DownloadItem(
            destination=os.path.join(WORK_DIR, "continent-cookie-cutter.gpkg"),
            source_filename=cc_filename,
            source_path=cc_path,
            source_version=cc_version,
        )

    @task(executor_config=POD_CONFIG_DEFAULT)
    def prepare_shared_resources(
        tag: str,
        coastline_result: list[dict],
        cookie_cutter_result: list[dict],
    ) -> dict[str, str]:
        """Combine download results into shared resources dict."""
        return {
            "work_dir": WORK_DIR,
            "coastline_gpkg": coastline_result[0]["path"],
            "cookie_cutter_gpkg": cookie_cutter_result[0]["path"],
            "tag": tag,
        }

    @task(executor_config=POD_CONFIG_DEFAULT)
    def prepare_continents(continents_input: str | None = None) -> list[dict]:
        """
        Prepare the list of continents to process.

        This is equivalent to the 'prepare' job in boundary-continent.yaml.

        Args:
            continents_input: Optional comma-separated list of continent slugs.
                              If empty, processes all continents.

        Returns:
            List of continent dictionaries with slug and name
        """
        if continents_input:
            slugs = [c.strip().lower() for c in continents_input.split(",") if c.strip()]
            slug_to_name = {c["slug"]: c["name"] for c in CONTINENTS}
            return [
                {"slug": slug, "name": slug_to_name[slug]}
                for slug in slugs
                if slug in slug_to_name
            ]
        return CONTINENTS.copy()

    @task(executor_config=POD_CONFIG_CONTINENT_EXTRACTION)
    def extract_and_upload_boundary(
        continent: dict,
        shared_resources: dict[str, str],
    ) -> dict:
        """
        Extract and upload boundary for a single continent.

        Requires: ogr2ogr, ogrinfo, python3 with pyproj (from IMAGE_GDAL)

        Args:
            continent: Continent dict with slug and name
            shared_resources: Dict with paths to shared resources

        Returns:
            Dict with extraction result
        """
        work_dir = shared_resources["work_dir"]
        tag = shared_resources["tag"]
        coastline_gpkg = shared_resources["coastline_gpkg"]
        cookie_cutter_gpkg = shared_resources["cookie_cutter_gpkg"]

        output_dir = os.path.join(work_dir, "output", tag)
        os.makedirs(output_dir, exist_ok=True)

        output_basename = os.path.join(output_dir, f"{continent['slug']}-latest.boundary")

        # Get the script path (the reusable extraction logic)
        script_path = Path(__file__).parent.parent / "utils" / "create_continent_boundary.sh"

        # Run the shell script
        result = subprocess.run(
            [
                str(script_path),
                continent["slug"],
                cookie_cutter_gpkg,
                coastline_gpkg,
                output_basename,
            ],
            capture_output=True,
            text=True,
            cwd=work_dir,
            env={**os.environ, "OGR_GEOJSON_MAX_OBJ_SIZE": "0"},
        )

        extraction_result = {
            "continent": continent,
            "success": result.returncode == 0,
            "gpkg_path": f"{output_basename}.gpkg" if result.returncode == 0 else None,
            "geojson_path": f"{output_basename}.geojson" if result.returncode == 0 else None,
            "parquet_path": f"{output_basename}.parquet" if result.returncode == 0 else None,
            "failure_reason": result.stderr if result.returncode != 0 else None,
        }

        # Upload if successful
        if extraction_result["success"]:
            slug = continent["slug"]
            tags = CONTINENT_TAGS + [slug]

            hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)
            base_path = f"boundaries/continents/{slug}"
            filename_base = f"{slug}-latest.boundary"

            upload_results = {}
            formats = [
                (extraction_result["geojson_path"], "geojson", "application/geo+json", "geojson"),
                (extraction_result["gpkg_path"], "gpkg", "application/geopackage+sqlite3", "geopackage"),
                (extraction_result["parquet_path"], "parquet", "application/vnd.apache.parquet", "geoparquet"),
            ]

            for local_path, ext, media_type, subfolder in formats:
                if local_path and Path(local_path).exists():
                    record = hook.upload(
                        bucket=R2_BUCKET,
                        category="boundary",
                        destination_filename=f"{filename_base}.{ext}",
                        destination_path=f"{base_path}/{subfolder}",
                        destination_version="1",
                        entity=slug,
                        extension=ext,
                        media_type=media_type,
                        source=local_path,
                        tags=tags + [subfolder],
                    )
                    upload_results[ext] = record["id"]

            extraction_result["uploaded"] = True
            extraction_result["upload_results"] = upload_results

        return extraction_result

    @task(
        trigger_rule="all_done",
        executor_config=POD_CONFIG_DEFAULT,
    )
    def cleanup(shared_resources: dict[str, str], results: list[dict]) -> None:
        """Clean up temporary files."""
        work_dir = shared_resources.get("work_dir")
        if work_dir and os.path.exists(work_dir):
            shutil.rmtree(work_dir, ignore_errors=True)

        # Log summary
        successful = sum(1 for r in results if r.get("success"))
        failed = len(results) - successful
        print(f"Extraction complete: {successful} successful, {failed} failed")
        for r in results:
            if not r.get("success"):
                print(f"  Failed: {r.get('continent', {}).get('slug')} - {r.get('failure_reason')}")

    # Define task flow
    tag = get_date_tag()
    coastline_result = download_coastline()
    cookie_cutter_result = download_cookie_cutter()
    shared = prepare_shared_resources(tag, coastline_result, cookie_cutter_result)
    continents = prepare_continents()

    # Dynamic task mapping - similar to GitHub's matrix strategy
    results = extract_and_upload_boundary.expand(
        continent=continents,
        shared_resources=[shared],  # Broadcast shared resources
    )

    cleanup(shared, results)
