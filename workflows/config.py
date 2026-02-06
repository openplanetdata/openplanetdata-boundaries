"""
Configuration constants for OpenPlanetData Boundaries Airflow DAGs.
"""

from kubernetes.client import models as k8s

# =============================================================================
# R2 Storage Configuration
# =============================================================================
R2_CONN_ID = "r2index-openplanetdata-production"
R2_BUCKET = "openplanetdata"

# Remote file references (path, filename, version)
PLANET_PBF_REF = ("osm/planet/pbf", "planet-latest.osm.pbf", "1")
PLANET_GOL_REF = ("osm/planet/gol", "planet-latest.osm.gol", "1")
COASTLINE_GPKG_REF = ("boundaries/coastline/geopackage", "planet-latest.coastline.gpkg", "1")
CONTINENT_COOKIE_CUTTER_REF = ("boundaries/continents/cookie-cutter", "continent-cookie-cutter.gpkg", "1")

# Output paths
BOUNDARIES_BASE_PATH = "/boundaries"
COASTLINE_OUTPUT_PATH = "/boundaries/coastline"
COUNTRIES_OUTPUT_PATH = "/boundaries/countries"
CONTINENTS_OUTPUT_PATH = "/boundaries/continents"

# =============================================================================
# Continents list
# =============================================================================
CONTINENTS = [
    {"slug": "africa", "name": "Africa"},
    {"slug": "antarctica", "name": "Antarctica"},
    {"slug": "asia", "name": "Asia"},
    {"slug": "europe", "name": "Europe"},
    {"slug": "north-america", "name": "North America"},
    {"slug": "oceania", "name": "Oceania"},
    {"slug": "south-america", "name": "South America"},
]

# =============================================================================
# Pod Configurations for KubernetesExecutor
# =============================================================================


def _pod_config(cpu_req, mem_req, cpu_lim, mem_lim):
    return {
        "pod_override": k8s.V1Pod(
            spec=k8s.V1PodSpec(
                containers=[
                    k8s.V1Container(
                        name="base",
                        resources=k8s.V1ResourceRequirements(
                            requests={"cpu": cpu_req, "memory": mem_req},
                            limits={"cpu": cpu_lim, "memory": mem_lim},
                        ),
                    )
                ]
            )
        )
    }


POD_CONFIG_DEFAULT = _pod_config("100m", "256Mi", "500m", "1Gi")
POD_CONFIG_GDAL = _pod_config("500m", "1Gi", "2", "4Gi")
POD_CONFIG_BOUNDARY_EXTRACTION = _pod_config("1", "2Gi", "4", "8Gi")
POD_CONFIG_CONTINENT_EXTRACTION = POD_CONFIG_BOUNDARY_EXTRACTION
POD_CONFIG_OSMCOASTLINE = _pod_config("2", "4Gi", "8", "16Gi")

# =============================================================================
# Task parallelism limits
# =============================================================================
MAX_PARALLEL_COUNTRIES = 5
MAX_PARALLEL_CONTINENTS = 3

# =============================================================================
# Metadata tags
# =============================================================================
COASTLINE_TAGS = ["coastline", "openstreetmap", "private"]
COUNTRY_TAGS = ["boundary", "country", "openstreetmap", "public"]
CONTINENT_TAGS = ["boundary", "continent", "openstreetmap", "public"]
