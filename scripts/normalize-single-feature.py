#!/usr/bin/env python3
"""
Normalize a GeoJSON file so that it contains a single Feature object.
If the input is a FeatureCollection, the first feature is preserved.
If the input is already a Feature, it is written back unchanged.
"""

import json
import sys


def normalize_geojson(path: str) -> None:
    with open(path, "r", encoding="utf-8") as fh:
        data = json.load(fh)

    geojson_type = data.get("type")

    if geojson_type == "FeatureCollection":
        features = data.get("features") or []
        if not features:
            raise SystemExit("No features found when converting to single feature")
        feature = features[0]
    elif geojson_type == "Feature":
        feature = data
    else:
        raise SystemExit(f"Unsupported GeoJSON type for conversion: {geojson_type}")

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(feature, fh, ensure_ascii=False, separators=(",", ":"))


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: normalize-single-feature.py <geojson_path>", file=sys.stderr)
        sys.exit(1)

    normalize_geojson(sys.argv[1])
