"""
Country Boundary DAG - Monthly country boundary extraction.

Schedule: Monthly 1st at 04:00 UTC
Source: .github/workflows/boundary-country.yaml + reusable-boundaries.yaml
Consumes Asset: coastline_gpkg (optional - waits if available)

Required tools (provided by WORKER_IMAGE):
- gol: OSM boundary queries
- ogr2ogr: Format conversions and geometry operations
- python3 with pyproj: Area calculations and metadata

Tasks:
1. download_planet_gol / download_coastline - Download shared resources
2. prepare_matrix - Build country matrix from countries data
3. extract_and_upload.expand() - Dynamic task mapping using shared extraction logic
4. cleanup - Remove downloaded files
"""

from __future__ import annotations

import os
import shutil
from datetime import datetime, timedelta

from airflow.sdk import DAG, Asset, task

from elaunira.airflow.providers.r2index.hooks import R2IndexHook
from elaunira.airflow.providers.r2index.operators import DownloadItem
from kubernetes.client import models as k8s
from openplanetdata.airflow.defaults import R2_BUCKET, R2INDEX_CONNECTION_ID
from openplanetdata.airflow.data.countries import COUNTRIES

COASTLINE_GPKG_REF = ("boundaries/coastline/geopackage", "planet-latest.coastline.gpkg", "v1")
COUNTRY_TAGS = ["boundary", "country", "openstreetmap", "public"]
MAX_PARALLEL_COUNTRIES = 5
PLANET_GOL_REF = ("osm/planet/gol", "planet-latest.osm.gol", "v1")

POD_CONFIG_DEFAULT = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "100m", "memory": "256Mi"},
                    limits={"cpu": "500m", "memory": "1Gi"},
                ),
            )]
        )
    )
}
POD_CONFIG_BOUNDARY_EXTRACTION = {
    "pod_override": k8s.V1Pod(
        spec=k8s.V1PodSpec(
            containers=[k8s.V1Container(
                name="base",
                resources=k8s.V1ResourceRequirements(
                    requests={"cpu": "1", "memory": "2Gi"},
                    limits={"cpu": "4", "memory": "8Gi"},
                ),
            )]
        )
    )
}

# Reference to coastline asset (consumed by this DAG)
coastline_gpkg = Asset(
    name="coastline_gpkg",
    uri=f"s3://{R2_BUCKET}/boundaries/coastline/geopackage/latest/planet-latest.coastline.gpkg",
)

WORK_DIR = "/data/openplanetdata/country_boundaries"


with DAG(
    catchup=False,
    dag_id="boundary_country",
    default_args={
        "depends_on_past": False,
        "execution_timeout": timedelta(hours=1),
        "owner": "openplanetdata",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    description="Monthly country boundary extraction from OSM",
    doc_md=__doc__,
    max_active_tasks=MAX_PARALLEL_COUNTRIES,
    schedule="0 4 1 * *",
    start_date=datetime(2024, 1, 1),
    tags=["boundary", "country", "osm", "monthly"],
) as dag:

    @task.r2index_download(
        bucket=R2_BUCKET,
        r2index_conn_id=R2INDEX_CONNECTION_ID,
        executor_config=POD_CONFIG_DEFAULT,
    )
    def download_planet_gol() -> DownloadItem:
        """Download planet GOL from R2."""
        gol_path, gol_filename, gol_version = PLANET_GOL_REF
        return DownloadItem(
            destination=os.path.join(WORK_DIR, "planet-latest.osm.gol"),
            source_filename=gol_filename,
            source_path=gol_path,
            source_version=gol_version,
        )

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

    @task(executor_config=POD_CONFIG_DEFAULT)
    def prepare_shared_resources(
        gol_result: list[dict],
        coastline_result: list[dict],
    ) -> dict[str, str]:
        """Combine download results into shared resources dict."""
        return {
            "work_dir": WORK_DIR,
            "planet_gol": gol_result[0]["path"],
            "coastline_gpkg": coastline_result[0]["path"],
        }

    @task(executor_config=POD_CONFIG_DEFAULT)
    def prepare_matrix(country_codes: str | None = None) -> list[dict]:
        """
        Prepare the matrix of countries to process.

        This is equivalent to the 'prepare' job in boundary-country.yaml.

        Args:
            country_codes: Optional comma-separated list of country codes.
                           If empty, processes all countries.

        Returns:
            List of country dictionaries with code, name, osm_query, has_coastline
        """
        countries = COUNTRIES

        if country_codes:
            codes = [c.strip().upper() for c in country_codes.split(",") if c.strip()]
        else:
            codes = sorted(countries.keys())

        matrix = []
        for code in codes:
            if code in countries:
                country = countries[code]
                tag_name = (
                    country["name"]
                    .lower()
                    .replace(" ", "-")
                    .replace(".", "")
                    .replace(",", "")
                )
                osm_query = f'a["ISO3166-1:alpha2"="{code}"]'
                matrix.append(
                    {
                        "code": code,
                        "name": country["name"],
                        "osm_query": osm_query,
                        "tag_name": tag_name,
                        "has_coastline": country.get("has_coastline", True),
                    }
                )

        return matrix

    @task(executor_config=POD_CONFIG_BOUNDARY_EXTRACTION)
    def extract_and_upload_boundary(
        country: dict,
        shared_resources: dict[str, str],
    ) -> dict:
        """
        Extract and upload boundary for a single country.

        Requires: gol, ogr2ogr, python3 with pyproj (from IMAGE_GOL)

        Args:
            country: Country dict with code, name, osm_query, has_coastline
            shared_resources: Dict with paths to shared resources

        Returns:
            Dict with extraction result
        """
        # Import extraction logic (available in the container)
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent / "utils"))
        from boundary_extraction import extract_country_boundary

        work_dir = shared_resources["work_dir"]
        planet_gol = shared_resources["planet_gol"]
        coastline_gpkg_path = shared_resources["coastline_gpkg"]

        # Create entity-specific output directory
        output_dir = os.path.join(work_dir, "output", country["code"])

        # Use the shared extraction logic
        result = extract_country_boundary(
            entity_code=country["code"],
            entity_name=country["name"],
            osm_query=country["osm_query"],
            has_coastline=country["has_coastline"],
            planet_gol=planet_gol,
            coastline_gpkg=coastline_gpkg_path,
            output_dir=output_dir,
        )

        extraction_result = {
            "country": country,
            "success": result.success,
            "geojson_path": result.geojson_path,
            "gpkg_path": result.gpkg_path,
            "parquet_path": result.parquet_path,
            "area_km2": result.area_km2,
            "failure_reason": result.failure_reason,
        }

        # Upload if successful
        if result.success:
            tag_name = country["tag_name"]
            tags = COUNTRY_TAGS + [tag_name]
            extra = {"area_km2": result.area_km2} if result.area_km2 else None

            hook = R2IndexHook(r2index_conn_id=R2INDEX_CONNECTION_ID)
            base_path = f"boundaries/countries/{country['code']}"
            filename_base = f"{country['code']}-latest.boundary"

            upload_results = {}
            formats = [
                (result.geojson_path, "geojson", "application/geo+json", "geojson"),
                (result.gpkg_path, "gpkg", "application/geopackage+sqlite3", "geopackage"),
                (result.parquet_path, "parquet", "application/vnd.apache.parquet", "geoparquet"),
            ]

            for local_path, ext, media_type, subfolder in formats:
                if local_path and Path(local_path).exists():
                    record = hook.upload(
                        bucket=R2_BUCKET,
                        category="boundary",
                        destination_filename=f"{filename_base}.{ext}",
                        destination_path=f"{base_path}/{subfolder}",
                        destination_version="1",
                        entity=country["code"],
                        extension=ext,
                        extra=extra,
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
                print(f"  Failed: {r.get('country', {}).get('code')} - {r.get('failure_reason')}")

    # Define task flow
    gol_result = download_planet_gol()
    coastline_result = download_coastline()
    shared = prepare_shared_resources(gol_result, coastline_result)
    countries = prepare_matrix()

    # Dynamic task mapping - similar to GitHub's matrix strategy
    results = extract_and_upload_boundary.expand(
        country=countries,
        shared_resources=[shared],  # Broadcast shared resources
    )

    cleanup(shared, results)
