#!/bin/bash
#
# Extract and dissolve continent boundaries using only ogr2ogr CLI
# Usage: ./create-continent-boundary.sh <continent> <continent_gpkg> <coastline_gpkg> [output_basename]

set -e

# Ensure GDAL accepts large GeoJSON features
export OGR_GEOJSON_MAX_OBJ_SIZE="${OGR_GEOJSON_MAX_OBJ_SIZE:-0}"

# Directory containing this script (used for helper scripts)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Available continents (kebab case)
CONTINENTS=("africa" "antarctica" "asia" "europe" "north-america" "oceania" "south-america")

usage() {
    cat << EOF
Usage: $0 <continent> <continent_gpkg> <coastline_gpkg> [output_basename]

Extract dissolved continent boundaries from coastline data.
Outputs in three formats: GeoPackage (.gpkg), GeoJSON (.geojson), and GeoParquet (.parquet)

Arguments:
  continent         Continent to extract (required)
  continent_gpkg    Path to continent cookie-cutter GeoPackage (required)
  coastline_gpkg    Path to coastline GeoPackage produced by osmcoastline (required)
  output_basename   Base name for output files without extension (default: <continent-kebab>.boundary)
                    Will create: <basename>.gpkg, <basename>.geojson, <basename>.parquet

Available continents:
  africa, antarctica, asia, europe, north-america, oceania, south-america

Examples:
  $0 africa continent-cookie-cutter.gpkg planet-coastline-latest.osm.gpkg
    → Creates: africa.boundary.gpkg, africa.boundary.geojson, africa.boundary.parquet

  $0 europe continent-cookie-cutter.gpkg planet-coastline-latest.osm.gpkg europe_border
    → Creates: europe_border.gpkg, europe_border.geojson, europe_border.parquet

EOF
    exit 1
}

# Check ogr2ogr
if ! command -v ogr2ogr &> /dev/null; then
    echo "Error: ogr2ogr not found. Install GDAL/OGR tools." >&2
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found. Install Python 3 to compute geodesic area." >&2
    exit 1
fi

# Parse arguments
[ $# -lt 3 ] && usage

CONTINENT="$1"
CONTINENT_GPKG="$2"
COASTLINE_GPKG="$3"
CONTINENT_LOWER=$(echo "$CONTINENT" | tr '[:upper:]' '[:lower:]')
CONTINENT_SLUG=${CONTINENT_LOWER//_/-}
DEFAULT_BASENAME="${CONTINENT_SLUG}.boundary"
OUTPUT_BASENAME="${4:-$DEFAULT_BASENAME}"
CONTINENT_DB_KEY=${CONTINENT_SLUG//-/_}

# Define output file paths
OUTPUT_GPKG="${OUTPUT_BASENAME}.gpkg"
OUTPUT_GEOJSON="${OUTPUT_BASENAME}.geojson"
OUTPUT_PARQUET="${OUTPUT_BASENAME}.parquet"

# Validate continent
VALID=0
for c in "${CONTINENTS[@]}"; do
    [ "$c" = "$CONTINENT_SLUG" ] && VALID=1 && break
done

[ $VALID -eq 0 ] && echo "Error: Invalid continent '$CONTINENT'" >&2 && exit 1

# Check files
[ ! -f "$CONTINENT_GPKG" ] && echo "Error: $CONTINENT_GPKG not found" >&2 && exit 1
[ ! -f "$COASTLINE_GPKG" ] && echo "Error: $COASTLINE_GPKG not found" >&2 && exit 1

echo "============================================" >&2
echo "Creating dissolved boundary for: $CONTINENT_SLUG" >&2
echo "============================================" >&2

# Create temporary directory for intermediate files
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

TEMP_CLIPPED="$TEMP_DIR/clipped.gpkg"
TEMP_DISSOLVED="$TEMP_DIR/dissolved.gpkg"

echo "" >&2
echo "[1/6] Clipping coastline by continent boundary..." >&2
START_TIME=$(date +%s)

# Step 1: Clip coastline polygons to GPKG (needed for SQL dissolve)
ogr2ogr -f GPKG "$TEMP_CLIPPED" "$COASTLINE_GPKG" land_polygons \
    -clipsrc "$CONTINENT_GPKG" \
    -clipsrclayer continent_cutter \
    -clipsrcwhere "continent = '$CONTINENT_DB_KEY'" \
    -nln clipped \
    2>&1 | grep -v "Warning" || true

CLIP_TIME=$(($(date +%s) - START_TIME))
CLIPPED_COUNT=$(ogrinfo -so -al "$TEMP_CLIPPED" 2>/dev/null | grep "Feature Count:" | awk '{print $3}')
CLIPPED_SIZE=$(ls -lh "$TEMP_CLIPPED" | awk '{print $5}')

echo "   ✓ Clipped $CLIPPED_COUNT polygons ($CLIPPED_SIZE) in ${CLIP_TIME}s" >&2

echo "" >&2
echo "[2/6] Dissolving polygons into single feature..." >&2
echo "   (This may take several minutes for large continents)" >&2
START_TIME=$(date +%s)

# Step 2: Dissolve using ogr2ogr with SQLite ST_Union on GPKG
ogr2ogr -f GPKG "$TEMP_DISSOLVED" "$TEMP_CLIPPED" \
    -dialect sqlite \
    -sql "SELECT ST_Union(geom) AS geom, '$CONTINENT_DB_KEY' AS continent FROM clipped" \
    -nln dissolved \
    2>&1 | grep -v "Warning" || true

DISSOLVE_TIME=$(($(date +%s) - START_TIME))

echo "   ✓ Dissolved in ${DISSOLVE_TIME}s" >&2

echo "" >&2
echo "[3/6] Calculating geodesic area..." >&2
AREA_START=$(date +%s)
TEMP_DISSOLVED_GEOJSON="$TEMP_DIR/dissolved.geojson"

# Export dissolved geometry to GeoJSON for accurate area calculation
ogr2ogr -f GeoJSON "$TEMP_DISSOLVED_GEOJSON" "$TEMP_DISSOLVED" dissolved \
    2>&1 | grep -v "Warning" || true

AREA_KM2=$(python3 "$SCRIPT_DIR/compute-area.py" "$TEMP_DISSOLVED_GEOJSON")
AREA_TIME=$(($(date +%s) - AREA_START))
echo "   ✓ Area: ${AREA_KM2} km² (computed in ${AREA_TIME}s)" >&2

echo "" >&2
echo "[4/6] Exporting to GeoPackage..." >&2
EXPORT_START=$(date +%s)

# Step 4: Export to GeoPackage with continent-named layer and area attribute
rm -f "$OUTPUT_GPKG"
ogr2ogr -f GPKG "$OUTPUT_GPKG" "$TEMP_DISSOLVED" \
    -dialect sqlite \
    -sql "SELECT geom, continent, CAST($AREA_KM2 AS REAL) AS area FROM dissolved" \
    -nln "$CONTINENT_SLUG" \
    2>&1 | grep -v "Warning" || true
GPKG_TIME=$(($(date +%s) - EXPORT_START))
GPKG_SIZE=$(ls -lh "$OUTPUT_GPKG" | awk '{print $5}')
echo "   ✓ Created $OUTPUT_GPKG ($GPKG_SIZE) in ${GPKG_TIME}s" >&2

echo "" >&2
echo "[5/6] Exporting to GeoJSON..." >&2
EXPORT_START=$(date +%s)

# Step 5: Convert to GeoJSON with continent-named layer
rm -f "$OUTPUT_GEOJSON"
ogr2ogr -f GeoJSON "$OUTPUT_GEOJSON" "$OUTPUT_GPKG" "$CONTINENT_SLUG" \
    -nln "$CONTINENT_SLUG" \
    2>&1 | grep -v "Warning" || true
if ! python3 "$SCRIPT_DIR/normalize-single-feature.py" "$OUTPUT_GEOJSON"; then
    echo "   ✗ Failed to normalize GeoJSON to single feature" >&2
    exit 1
fi
GEOJSON_TIME=$(($(date +%s) - EXPORT_START))
GEOJSON_SIZE=$(ls -lh "$OUTPUT_GEOJSON" | awk '{print $5}')
echo "   ✓ Created $OUTPUT_GEOJSON ($GEOJSON_SIZE) in ${GEOJSON_TIME}s" >&2

echo "" >&2
echo "[6/6] Exporting to GeoParquet..." >&2
EXPORT_START=$(date +%s)

# Step 6: Convert to GeoParquet with continent-named layer
rm -f "$OUTPUT_PARQUET"
ogr2ogr -f Parquet "$OUTPUT_PARQUET" "$OUTPUT_GPKG" "$CONTINENT_SLUG" \
    -nln "$CONTINENT_SLUG" \
    2>&1 | grep -v "Warning" || true

if [ -f "$OUTPUT_PARQUET" ]; then
    PARQUET_TIME=$(($(date +%s) - EXPORT_START))
    PARQUET_SIZE=$(ls -lh "$OUTPUT_PARQUET" | awk '{print $5}')
    echo "   ✓ Created $OUTPUT_PARQUET ($PARQUET_SIZE) in ${PARQUET_TIME}s" >&2
else
    echo "   ⚠ GeoParquet export failed (GDAL may not support Parquet format)" >&2
    PARQUET_TIME=0
fi

# Get final stats
if [ -f "$OUTPUT_GEOJSON" ]; then
    FEATURE_COUNT=$(grep -o '"type": "Feature"' "$OUTPUT_GEOJSON" | wc -l)
    TOTAL_TIME=$((CLIP_TIME + DISSOLVE_TIME + AREA_TIME + GPKG_TIME + GEOJSON_TIME + PARQUET_TIME))

    echo "" >&2
    echo "============================================" >&2
    echo "✓ SUCCESS!" >&2
    echo "============================================" >&2
    echo "Output files:" >&2
    echo "  GPKG:       $OUTPUT_GPKG ($GPKG_SIZE)" >&2
    echo "  GeoJSON:    $OUTPUT_GEOJSON ($GEOJSON_SIZE)" >&2
    if [ -f "$OUTPUT_PARQUET" ]; then
        echo "  GeoParquet: $OUTPUT_PARQUET ($PARQUET_SIZE)" >&2
    fi
    echo "" >&2
    echo "Features:     $FEATURE_COUNT (dissolved from $CLIPPED_COUNT polygons)" >&2
    echo "Area (km²):   $AREA_KM2" >&2
    echo "Total time:   ${TOTAL_TIME}s" >&2
    echo "" >&2
else
    echo "" >&2
    echo "✗ Failed to create output files" >&2
    exit 1
fi
