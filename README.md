# Debezium CockroachDB Examples

End-to-end CDC replication examples using [Debezium](https://debezium.io/) connectors with CockroachDB.

## Demos

| Demo                          | Source      | Target      | Description                                                                                                                                                             |
|-------------------------------|-------------|-------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [crdb-to-crdb](crdb-to-crdb/) | CockroachDB | CockroachDB | Full CDC replication using the Debezium CockroachDB source connector with enriched changefeeds. Includes multi-table, schema evolution, and incremental snapshot demos. |
| [pg-to-crdb](pg-to-crdb/)     | PostgreSQL  | CockroachDB | PostgreSQL partitioned table migration using the Debezium PostgreSQL source connector with `ByLogicalTableRouter` SMT to merge partition topics.                        |

## Quick Start

Each demo is self-contained. Navigate to the demo folder and run:

```bash
cd crdb-to-crdb && ./run-demo.sh
```

or

```bash
cd pg-to-crdb && ./run-demo.sh
```

## Prerequisites

- Docker and Docker Compose (or Podman)

See individual demo READMEs for additional details.
