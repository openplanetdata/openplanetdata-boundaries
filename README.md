# OpenPlanetData Boundaries

Boundary datasets derived from OpenStreetMap and published as GeoPackage,
GeoJSON, and GeoParquet on R2.

All geometries are in EPSG:4326 (WGS84). Areas are computed in EPSG:6933
(equal-area projection) and reported in km².

## Datasets

- [Coastline](#coastline) — global land/water coastline polygons
- [Continents](#continents) — per-continent dissolved land boundaries
- [Countries](#countries) — per-country dissolved land boundaries
- [Regions](#regions) — per-ISO 3166-2 region dissolved land boundaries

---

### Coastline

Workflow: `workflows/planet_coastline_dag.py`
Source: planet OSM PBF processed with `osmcoastline -p both`.
R2 path: `boundaries/coastline/{geopackage,geojson,geoparquet}/v2/`

Each feature is either a land or water polygon emitted by `osmcoastline`.

| Field           | Type         | Description                                                             |
| --------------- | ------------ | ----------------------------------------------------------------------- |
| `feature_class` | String       | `land` for entries from `land_polygons`, `water` from `water_polygons`. |
| `geometry`      | MultiPolygon | Coastline polygon (EPSG:4326).                                          |
| `id`            | Integer      | Original identifier from the `osmcoastline` source layer.               |

---

### Continents

Workflow: `workflows/boundary_continent_dag.py`
Source: clipped from the coastline GPKG using a per-continent cookie cutter,
then dissolved.
R2 path: `boundaries/continents/{slug}/{geopackage,geojson,geoparquet}/v2/`
Aggregate: `boundaries/continents/planet/...` (all continents in one file).

| Field      | Type           | Description                                                              |
| ---------- | -------------- | ------------------------------------------------------------------------ |
| `area`     | Real           | Geodesic area in km² (EPSG:6933, rounded to 2 decimals).                 |
| `code`     | String         | Two-letter continent code (`AF`, `AN`, `AS`, `EU`, `NA`, `OC`, `SA`).    |
| `geometry` | (Multi)Polygon | Dissolved continent land boundary (EPSG:4326).                           |
| `name`     | String         | Capital-case display name (e.g. `Africa`, `North America`).              |
| `slug`     | String         | Lower-case kebab-case identifier (e.g. `africa`, `north-america`).       |

Continents:

| Code | Slug            | Name           |
| ---- | --------------- | -------------- |
| AF   | `africa`        | Africa         |
| AN   | `antarctica`    | Antarctica     |
| AS   | `asia`          | Asia           |
| EU   | `europe`        | Europe         |
| NA   | `north-america` | North America  |
| OC   | `oceania`       | Oceania        |
| SA   | `south-america` | South America  |

---

### Countries

Workflow: `workflows/boundary_country_dag.py`
Source: country boundary extracted from OSM (`a["ISO3166-1:alpha2"="XX"]`)
via `gol query`, clipped against the coastline, then dissolved.
R2 path: `boundaries/countries/{alpha2}/{geopackage,geojson,geoparquet}/v2/`
Aggregate: `boundaries/countries/planet/...` (all countries in one file).

| Field      | Type           | Description                                                |
| ---------- | -------------- | ---------------------------------------------------------- |
| `area`     | Real           | Geodesic area in km² (EPSG:6933, rounded to 2 decimals).   |
| `code`     | String         | ISO 3166-1 alpha-2 country code (e.g. `FR`, `US`, `JP`).   |
| `geometry` | (Multi)Polygon | Dissolved country land boundary (EPSG:4326).               |
| `name`     | String         | English country name (e.g. `France`, `United States`).    |
| `slug`     | String         | Lower-case alpha-2 identifier (e.g. `fr`, `us`, `jp`).     |

---

### Regions

Workflow: `workflows/boundary_region_dag.py`
Source: all `a["ISO3166-2"]` features from OSM via `gol query`, split per
region code, clipped against the coastline, then dissolved.
R2 path: `boundaries/regions/{code}/{geopackage,geojson,geoparquet}/v2/`
Aggregate: `boundaries/regions/planet/...` (all regions in one file).

The region code uses `-` instead of `:` as the country/subdivision
separator (e.g. `FR-IDF` for ISO `FR:IDF`).

| Field      | Type           | Description                                                                  |
| ---------- | -------------- | ---------------------------------------------------------------------------- |
| `area`     | Real           | Geodesic area in km² (EPSG:6933, rounded to 2 decimals).                     |
| `code`     | String         | ISO 3166-2 region code with `-` separator (e.g. `FR-IDF`, `US-CA`, `JP-13`). |
| `geometry` | (Multi)Polygon | Dissolved region land boundary (EPSG:4326).                                  |
| `name`     | String         | Region name from the OSM `name` tag (may be empty if not set in OSM).        |
| `slug`     | String         | Lower-case region identifier (e.g. `fr-idf`, `us-ca`, `jp-13`).              |
