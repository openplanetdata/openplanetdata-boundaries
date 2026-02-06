"""
Configuration constants for OpenPlanetData Boundaries Airflow DAGs.
"""

from openplanetdata.airflow.utils.k8s import (
    POD_DEFAULT,
    POD_LARGE,
    POD_MEDIUM,
    POD_XLARGE,
    create_pod_spec,
)

# =============================================================================
# R2 Storage Configuration
# =============================================================================
R2_CONN_ID = "r2_default"
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
# Pod Configurations (re-exported from shared package for convenience)
# =============================================================================
POD_CONFIG_DEFAULT = POD_DEFAULT
POD_CONFIG_GDAL = POD_MEDIUM
POD_CONFIG_OSMCOASTLINE = POD_XLARGE
POD_CONFIG_BOUNDARY_EXTRACTION = POD_LARGE
POD_CONFIG_CONTINENT_EXTRACTION = POD_LARGE

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
