#!/usr/bin/env python3

import re
import sqlite3
import sys
from pathlib import Path


def _query_gpkg_errors(gpkg_path: Path) -> dict[str, list[tuple]]:
    """Query error tables from osmcoastline output GeoPackage."""
    result: dict[str, list[tuple]] = {"error_lines": [], "error_points": []}
    try:
        conn = sqlite3.connect(str(gpkg_path))
        cursor = conn.cursor()
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('error_lines', 'error_points')"
        )
        tables = {row[0] for row in cursor.fetchall()}
        for table in ("error_lines", "error_points"):
            if table in tables:
                cursor.execute(f"SELECT osm_id, error FROM {table}")  # noqa: S608
                result[table] = cursor.fetchall()
        conn.close()
    except Exception as exc:
        print(f"Warning: failed to query GPKG for error details: {exc}", file=sys.stderr)
    return result


def main(log_path_str: str, gpkg_path_str: str | None = None) -> int:
    """Parse osmcoastline log and optionally query GPKG for error details.

    Returns the number of errors reported by osmcoastline, or -1 on read failure.
    """
    log_path = Path(log_path_str)
    try:
        with log_path.open(encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except OSError as exc:
        print(f"Failed to read log file {log_path}: {exc}", file=sys.stderr)
        return -1

    warning_pat = re.compile(r"warning", re.IGNORECASE)
    error_pat = re.compile(r"error", re.IGNORECASE)
    summary_pat = re.compile(r"There were (\d+) (warnings?|errors?)\.", re.IGNORECASE)
    ring_pat = re.compile(r"ring_id=(\d+)")
    rel_offset = 6_000_000_000_000_000

    def describe_ring(raw: str) -> str:
        rid = int(raw)
        if rid >= rel_offset:
            rel_id = rid - rel_offset
            return f"relation {rel_id} https://www.openstreetmap.org/relation/{rel_id} (ring_id={rid})"
        return f"way {rid} https://www.openstreetmap.org/way/{rid} (ring_id={rid})"

    summary = {"warnings": 0, "errors": 0}
    warnings = []
    errors = []

    for idx, line in enumerate(lines, 1):
        stripped = line.strip()
        if not stripped:
            continue

        match = summary_pat.match(stripped)
        if match:
            count = int(match.group(1))
            kind = match.group(2).lower()
            if "warning" in kind:
                summary["warnings"] = count
            else:
                summary["errors"] = count
            continue

        entry = None
        if warning_pat.search(stripped):
            entry = warnings
        elif error_pat.search(stripped):
            entry = errors

        if entry is not None:
            entities = []
            ring_match = ring_pat.search(stripped)
            if ring_match:
                entities.append(describe_ring(ring_match.group(1)))
            entry.append((idx, stripped, entities))

    def render(entries, label, reported_total):
        rendered_lines = []
        if entries:
            rendered_lines.append(f"{label} ({len(entries)}):")
            for idx, text, entities in entries[:20]:
                rendered_lines.append(f"  • [line {idx}] {text}")
                for ent in entities:
                    rendered_lines.append(f"      ↳ {ent}")
            if len(entries) > 20:
                rendered_lines.append(f"  … (showing first 20 of {len(entries)} {label.lower()})")
        else:
            if reported_total:
                rendered_lines.append(f"{label}: no detailed {label.lower()} lines captured in log.")
            else:
                rendered_lines.append(f"{label}: none.")
        return rendered_lines

    # Query GPKG for error details
    gpkg_errors: dict[str, list[tuple]] = {"error_lines": [], "error_points": []}
    if gpkg_path_str:
        gpkg_path = Path(gpkg_path_str)
        if gpkg_path.exists():
            gpkg_errors = _query_gpkg_errors(gpkg_path)

    ERROR_DESCRIPTIONS = {
        "added_line": "synthetic segment inserted by osmcoastline to close a gap",
        "direction": "coastline way going the wrong direction (land/water reversed)",
        "fixed_end_point": "point where osmcoastline closed a gap in the coastline",
        "intersection": "two coastline segments crossing each other",
        "questionable": "coastline way flagged as suspicious (e.g. too small or oddly shaped)",
        "tagged_node": "coastline node with extra OSM tags (informational)",
    }

    def _osm_link(osm_id, osm_type):
        if osm_id == 0:
            return ""
        if osm_id >= rel_offset:
            rel_id = osm_id - rel_offset
            return f" https://www.openstreetmap.org/relation/{rel_id}"
        return f" https://www.openstreetmap.org/{osm_type}/{osm_id}"

    def render_gpkg_diagnostics():
        rendered_lines = []
        osm_types = {"error_points": "node", "error_lines": "way"}
        for table in ("error_points", "error_lines"):
            entries = gpkg_errors[table]
            if entries:
                rendered_lines.append(f"  {table} ({len(entries)}):")
                osm_type = osm_types[table]
                for osm_id, error in entries[:20]:
                    link = _osm_link(osm_id, osm_type)
                    desc = ERROR_DESCRIPTIONS.get(error, "")
                    suffix = f" — {desc}" if desc else ""
                    rendered_lines.append(f"    • osm_id={osm_id}{link} : {error}{suffix}")
                if len(entries) > 20:
                    rendered_lines.append(f"    … (showing first 20 of {len(entries)})")
        return rendered_lines

    output_lines = [
        "========================================",
        f"osmcoastline analysis for {log_path.name}",
        "========================================",
        f"Summary: {summary['warnings']} warnings, {summary['errors']} errors reported by osmcoastline.",
        "",
        *render(warnings, "Warnings", summary["warnings"]),
        "",
        *render(errors, "Errors", summary["errors"]),
    ]

    gpkg_detail_lines = render_gpkg_diagnostics()
    if gpkg_detail_lines:
        output_lines.append("")
        output_lines.append("GPKG diagnostics (data quality findings auto-handled by osmcoastline):")
        output_lines.extend(gpkg_detail_lines)

    output_lines.append("")
    output_lines.append("End of osmcoastline report")

    print("\n".join(output_lines))
    return summary["errors"]


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: osmcoastline-report.py <log_path> [gpkg_path]", file=sys.stderr)
        sys.exit(1)
    gpkg = sys.argv[2] if len(sys.argv) > 2 else None
    sys.exit(1 if main(sys.argv[1], gpkg) > 0 else 0)
