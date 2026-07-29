# Third-Party Notices

ClearFeature depends on the following direct dependencies. This is a factual
inventory of upstream license identifiers as declared by each project; it is not
legal advice, and license texts ship with the packages themselves.

## Runtime (core)

| Package | License |
|---|---|
| PyYAML | MIT |
| prometheus-client | Apache-2.0 |

## Optional extras

| Extra | Package | License |
|---|---|---|
| api | fastapi | MIT |
| api | uvicorn | BSD-3-Clause |
| api | pydantic | MIT |
| storage | minio | Apache-2.0 |
| postgres | psycopg (binary) | **LGPL-3.0** |
| postgres | psycopg-pool | **LGPL-3.0** |
| online | redis | MIT |
| kafka | confluent-kafka | Apache-2.0 |
| redpanda | kafka | BSL |

## Development

pytest (MIT), ruff (MIT), hatchling build backend (MIT).

## Note on LGPL dependencies

`psycopg` and `psycopg-pool` are licensed under LGPL-3.0. ClearFeature uses them as
ordinary imported Python libraries (dynamic use, unmodified). Downstream
redistributors should perform their own license review; flag any concerns to the
maintainers before packaging ClearFeature into other distributions.

The full transitive dependency set and exact pinned versions are recorded in
`uv.lock`.
