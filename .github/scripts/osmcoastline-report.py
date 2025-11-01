#!/usr/bin/env python3

import json
import re
import sys
from pathlib import Path

def main(log_path_str: str) -> int:
    log_path = Path(log_path_str)
    try:
        with log_path.open(encoding="utf-8", errors="replace") as fh:
            lines = fh.readlines()
    except OSError as exc:
        print(f"Failed to read log file {log_path}: {exc}", file=sys.stderr)
        return 1

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
                rendered_lines.append(f"{label}: no detailed {label.lower()} lines captured, but osmcoastline reported {reported_total}.")
            else:
                rendered_lines.append(f"{label}: none.")
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
        "",
        "End of osmcoastline report",
    ]

    print("\n".join(output_lines))
    return 0


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: osmcoastline-report.py <log_path>", file=sys.stderr)
        sys.exit(1)
    sys.exit(main(sys.argv[1]))
