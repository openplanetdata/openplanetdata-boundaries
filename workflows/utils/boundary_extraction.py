#!/usr/bin/env python3
"""
Core boundary extraction logic.
Extracts country/entity boundaries from OSM data using gol queries.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

try:
    from workflows.utils.normalize_single_feature import normalize_geojson
except ImportError:
    from normalize_single_feature import normalize_geojson

logger = logging.getLogger(__name__)


@dataclass
class ExtractionResult:
    """Result of a boundary extraction."""

    success: bool
    geojson_path: str | None = None
    gpkg_path: str | None = None
    parquet_path: str | None = None
    feature_count: int = 0
    area_km2: float | None = None
    failure_reason: str | None = None


def run_command(cmd: list[str], cwd: str | None = None) -> tuple[int, str, str]:
    """Run a shell command and return exit code, stdout, stderr."""
    result = subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
    )
    return result.returncode, result.stdout, result.stderr


def compute_area_km2(geojson_path: str) -> float | None:
    """Compute area in km² using ogr2ogr with EPSG:6933 equal-area projection."""
    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_gpkg = os.path.join(tmpdir, "tmp.gpkg")
        csv_path = os.path.join(tmpdir, "area.csv")

        # Convert GeoJSON to GPKG with known layer name
        rc, _, _ = run_command([
            "ogr2ogr", "-f", "GPKG", tmp_gpkg, geojson_path,
            "--config", "OGR_GEOJSON_MAX_OBJ_SIZE", "0",
            "-nln", "boundary",
        ])
        if rc != 0:
            return None

        # Compute area via equal-area projection to CSV
        rc, _, _ = run_command([
            "ogr2ogr", "-f", "CSV", csv_path, tmp_gpkg,
            "-dialect", "sqlite",
            "-sql", "SELECT ROUND(ST_Area(ST_Transform(ST_Union(geom), 6933)) / 1000000.0, 2) AS area_km2 FROM boundary",
        ])
        if rc != 0:
            return None

        with open(csv_path) as f:
            lines = f.read().strip().splitlines()
            if len(lines) >= 2:
                return float(lines[1])

    return None


def extract_boundary_with_gol(
    planet_gol: str,
    osm_query: str,
    output_path: str,
) -> tuple[bool, str]:
    """
    Extract boundary using gol query.

    Args:
        planet_gol: Path to planet GOL file
        osm_query: OSM query string (e.g., 'a["ISO3166-1:alpha2"="FR"]')
        output_path: Path for output GeoJSON

    Returns:
        Tuple of (success, error_message)
    """
    cmd = ["gol", "query", planet_gol, osm_query, "-f", "geojson"]

    try:
        with open(output_path, "w") as f:
            result = subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, text=True)

        if result.returncode != 0:
            return False, f"gol query failed: {result.stderr}"

        return True, ""
    except Exception as e:
        return False, str(e)


def clip_with_coastline(
    input_geojson: str,
    coastline_gpkg: str,
    output_geojson: str,
) -> bool:
    """
    Clip geometry with coastline data using ogr2ogr.

    Args:
        input_geojson: Path to input GeoJSON
        coastline_gpkg: Path to coastline GeoPackage
        output_geojson: Path for output GeoJSON

    Returns:
        True if successful
    """
    cmd = [
        "ogr2ogr",
        "-f",
        "GeoJSON",
        output_geojson,
        coastline_gpkg,
        "land_polygons",
        "-clipsrc",
        input_geojson,
        "-q",
    ]

    returncode, _, stderr = run_command(cmd)
    return returncode == 0


def dissolve_geometry(
    input_geojson: str,
    output_geojson: str,
    layer_name: str = "land_polygons",
) -> bool:
    """
    Dissolve geometry using ogr2ogr with SQLite dialect.

    Args:
        input_geojson: Path to input GeoJSON
        output_geojson: Path for output GeoJSON
        layer_name: Name of the layer in input

    Returns:
        True if successful
    """
    sql = f"SELECT ST_CollectionExtract(ST_UnaryUnion(ST_Collect(geometry)), 3) AS geometry FROM {layer_name}"

    cmd = [
        "ogr2ogr",
        "-f",
        "GeoJSON",
        output_geojson,
        input_geojson,
        "-dialect",
        "sqlite",
        "-sql",
        sql,
        "-q",
    ]

    returncode, _, _ = run_command(cmd)
    return returncode == 0


def add_properties_to_geojson(
    geojson_path: str,
    entity_type: str,
    entity_code: str,
    entity_name: str,
    area_km2: float,
) -> bool:
    """
    Add properties (area, name, code) to GeoJSON feature.

    Args:
        geojson_path: Path to GeoJSON file
        entity_type: Type of entity (country, continent)
        entity_code: Entity code
        entity_name: Entity name
        area_km2: Area in km²

    Returns:
        True if successful
    """
    try:
        with open(geojson_path, "r") as f:
            data = json.load(f)

        # Handle both Feature and FeatureCollection
        if data.get("type") == "FeatureCollection":
            features = data.get("features", [])
            if not features:
                return False
            feature = features[0]
        elif data.get("type") == "Feature":
            feature = data
        else:
            return False

        if "properties" not in feature or feature["properties"] is None:
            feature["properties"] = {}

        feature["properties"]["area"] = area_km2
        feature["properties"]["name"] = entity_name

        if entity_type == "country":
            code_upper = entity_code.upper()
            feature["properties"]["ISO3166-1:alpha2"] = code_upper
        elif entity_type == "continent":
            feature["properties"]["code"] = entity_code.upper()

        with open(geojson_path, "w") as f:
            json.dump(data, f)

        return True
    except Exception as e:
        logger.error(f"Failed to add properties: {e}")
        return False


def convert_to_geopackage(
    input_geojson: str,
    output_gpkg: str,
    layer_name: str,
) -> bool:
    """Convert GeoJSON to GeoPackage."""
    cmd = [
        "ogr2ogr",
        "--config",
        "OGR_GEOJSON_MAX_OBJ_SIZE",
        "0",
        "-f",
        "GPKG",
        output_gpkg,
        input_geojson,
        "-nln",
        layer_name,
        "-overwrite",
    ]
    returncode, _, _ = run_command(cmd)
    return returncode == 0


def convert_to_geoparquet(
    input_geojson: str,
    output_parquet: str,
    layer_name: str,
) -> bool:
    """Convert GeoJSON to GeoParquet."""
    cmd = [
        "ogr2ogr",
        "-f",
        "Parquet",
        output_parquet,
        input_geojson,
        "--config",
        "OGR_GEOJSON_MAX_OBJ_SIZE",
        "0",
        "-lco",
        "COMPRESSION=ZSTD",
        "-lco",
        "GEOMETRY_ENCODING=GEOARROW",
        "-lco",
        "ROW_GROUP_SIZE=65536",
        "-nln",
        layer_name,
    ]
    returncode, _, _ = run_command(cmd)
    return returncode == 0


def extract_country_boundary(
    entity_code: str,
    entity_name: str,
    osm_query: str,
    has_coastline: bool,
    planet_gol: str,
    coastline_gpkg: str,
    output_dir: str,
) -> ExtractionResult:
    """
    Extract a country boundary from OSM data.

    Args:
        entity_code: Country ISO code (e.g., "FR")
        entity_name: Country name
        osm_query: OSM query string
        has_coastline: Whether to clip with coastline
        planet_gol: Path to planet GOL file
        coastline_gpkg: Path to coastline GeoPackage
        output_dir: Directory for output files

    Returns:
        ExtractionResult with paths to generated files
    """
    os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"

    output_dir_path = Path(output_dir)
    output_dir_path.mkdir(parents=True, exist_ok=True)

    output_geojson = str(output_dir_path / f"{entity_code}.boundary.geojson")
    output_gpkg = str(output_dir_path / f"{entity_code}.boundary.gpkg")
    output_parquet = str(output_dir_path / f"{entity_code}.boundary.parquet")

    with tempfile.TemporaryDirectory() as tmpdir:
        raw_geojson = os.path.join(tmpdir, f"{entity_code}_raw.geojson")
        clipped_geojson = os.path.join(tmpdir, f"{entity_code}_clipped.geojson")
        dissolved_geojson = os.path.join(tmpdir, f"{entity_code}_dissolved.geojson")

        # Step 1: Extract boundary with gol query
        logger.info(f"Extracting boundary for {entity_code} ({entity_name})...")
        success, error = extract_boundary_with_gol(planet_gol, osm_query, raw_geojson)
        if not success:
            return ExtractionResult(
                success=False,
                failure_reason=f"Failed to extract boundary: {error}",
            )

        # Validate extraction
        try:
            with open(raw_geojson) as f:
                data = json.load(f)
            feature_count = len(data.get("features", []))
            if feature_count == 0:
                return ExtractionResult(
                    success=False,
                    failure_reason="No features found in gol query result",
                )
        except Exception as e:
            return ExtractionResult(
                success=False,
                failure_reason=f"Failed to parse extraction result: {e}",
            )

        # Step 2: Clip with coastline (if coastal)
        working_file = raw_geojson
        clipped = False

        if has_coastline:
            logger.info(f"Clipping {entity_code} with coastline data...")
            if clip_with_coastline(raw_geojson, coastline_gpkg, clipped_geojson):
                working_file = clipped_geojson
                clipped = True
            else:
                logger.warning(f"Coastline clipping failed for {entity_code}, using raw boundary")

        # Step 3: Dissolve geometry (only if clipped)
        if clipped:
            logger.info(f"Dissolving geometry for {entity_code}...")
            if not dissolve_geometry(working_file, dissolved_geojson, "land_polygons"):
                return ExtractionResult(
                    success=False,
                    failure_reason="Failed to dissolve geometry",
                )
            final_file = dissolved_geojson
        else:
            final_file = working_file

        # Step 4: Copy to output
        shutil.copy(final_file, output_geojson)

        # Step 5: Compute area
        logger.info(f"Computing area for {entity_code}...")
        area_km2 = compute_area_km2(output_geojson)
        if area_km2 is None:
            return ExtractionResult(
                success=False,
                failure_reason="Failed to compute area",
            )

        # Step 6: Add properties
        layer_name = entity_code.upper()
        if not add_properties_to_geojson(
            output_geojson, "country", entity_code, entity_name, area_km2
        ):
            return ExtractionResult(
                success=False,
                failure_reason="Failed to add properties",
            )

        # Step 7: Normalize to single feature
        try:
            normalize_geojson(output_geojson)
        except Exception as e:
            return ExtractionResult(
                success=False,
                failure_reason=f"Failed to normalize GeoJSON: {e}",
            )

        # Step 8: Convert to GeoPackage
        logger.info(f"Converting {entity_code} to GeoPackage...")
        if not convert_to_geopackage(output_geojson, output_gpkg, layer_name):
            return ExtractionResult(
                success=False,
                failure_reason="Failed to convert to GeoPackage",
            )

        # Step 9: Convert to GeoParquet
        logger.info(f"Converting {entity_code} to GeoParquet...")
        if not convert_to_geoparquet(output_geojson, output_parquet, layer_name):
            return ExtractionResult(
                success=False,
                failure_reason="Failed to convert to GeoParquet",
            )

        logger.info(f"Successfully extracted boundary for {entity_code}")

        return ExtractionResult(
            success=True,
            geojson_path=output_geojson,
            gpkg_path=output_gpkg,
            parquet_path=output_parquet,
            feature_count=1,
            area_km2=area_km2,
        )


if __name__ == "__main__":
    import sys

    logging.basicConfig(level=logging.INFO)

    if len(sys.argv) < 7:
        print(
            "Usage: boundary_extraction.py <code> <name> <osm_query> <has_coastline> <planet_gol> <coastline_gpkg> <output_dir>"
        )
        sys.exit(1)

    result = extract_country_boundary(
        entity_code=sys.argv[1],
        entity_name=sys.argv[2],
        osm_query=sys.argv[3],
        has_coastline=sys.argv[4].lower() == "true",
        planet_gol=sys.argv[5],
        coastline_gpkg=sys.argv[6],
        output_dir=sys.argv[7] if len(sys.argv) > 7 else ".",
    )

    if result.success:
        print(f"Success: {result.geojson_path}")
        print(f"Area: {result.area_km2} km²")
    else:
        print(f"Failed: {result.failure_reason}")
        sys.exit(1)
