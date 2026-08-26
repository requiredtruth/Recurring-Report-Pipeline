# Recurring Report Pipeline

A local-first desktop and CLI tool for recurring CSV reports that need to **fail visibly instead of silently delivering bad output**. It validates changing source columns, normalizes typed fields, filters and aggregates rows, applies row-count quality gates, writes CSV/JSON/HTML reports atomically, records durable run evidence, and copies verified deliverables into local outboxes.

```bash
./run.sh                         # install/repair, then launch the desktop GUI
./cli.sh --data-dir demo demo    # one-command synthetic end-to-end proof
```

The demo uses four synthetic rows, needs no account or network service, and exercises ingest → validation → transformation → quality gate → three outputs → local delivery.

## Why this exists

General orchestrators such as [Apache Airflow](https://github.com/apache/airflow), [Dagster](https://github.com/dagster-io/dagster), and [Prefect](https://github.com/PrefectHQ/prefect) cover broad workflow estates. [csvkit](https://github.com/wireservice/csvkit) provides excellent composable CSV commands. This project occupies a narrower space: a zero-runtime-dependency Python engine plus SQLite evidence store and desktop control panel for bounded recurring report jobs, with schema drift checks, blank-output blocking, atomic artifacts, and contained local delivery in one application. It is not a replacement for those projects.

Useful problem searches this repository directly addresses:

- `scheduled CSV report silently stopped`
- `block blank report delivery when source columns change`
- `local recurring CSV pipeline with run history`
- `normalize aggregate CSV to HTML report without a server`

## Install and command contract

Python 3.11+ is required. `install.sh` creates a project-local virtual environment, installs the package and PySide6, then constructs the full GUI offscreen as a readiness check. It is repeat-safe.

```bash
./install.sh                     # install or repair all dependencies
./run.sh                         # GUI: status, imports, due runs, safe demo
./cli.sh --help                  # CLI help
./cli.sh --data-dir work demo
./cli.sh --data-dir work register pipeline.json
./cli.sh --data-dir work run-due
./cli.sh --data-dir work scheduler --poll-seconds 30
./cli.sh --data-dir work status
./test.sh
```

Run the foreground `scheduler` from the service manager already trusted on the host. The project does not install persistence, modify cron, or open a network listener.

## Configuration

Configurations are strict UTF-8 JSON. See [`examples/weekly-summary.json`](examples/weekly-summary.json). A job may define:

- a contained CSV path under `<data-dir>/inputs`;
- source-to-output column mapping with `string`, `integer`, decimal `number`, `boolean`, or ISO `date` conversion;
- bounded `eq`, `ne`, comparison, and `contains` filters;
- deterministic group-by with `count`, `sum`, `average`, `min`, and `max`;
- stable multi-column sort;
- CSV, JSON, and/or standalone HTML outputs;
- minimum/maximum output rows and required source headers;
- contained `local_copy` delivery outboxes;
- interval, daily, weekly, or monthly UTC scheduling.

Registered config snapshots, run claims, statuses, input hashes, output manifests, safe errors, and meaningful events are stored in SQLite with WAL, foreign keys, a busy timeout, and a schema version. Each schedule slot is unique per pipeline. Stale `running` records are marked failed on restart. Outputs are staged and renamed only after validation and hashing.

## Failure behavior

Exit `0` means success, `1` an execution failure, and `2` invalid input or an unknown pipeline. Machine-readable results go to stdout; bounded errors go to stderr. Missing files, malformed UTF-8/CSV, absent required headers, duplicate headers, conversion errors, oversized inputs/rows/cells, empty results, and row-count drift block publication. Failed runs remain visible with a stable code and safe message.

## Limits and honest scope

Version 0.1.0 reads one local UTF-8 CSV per pipeline and provides local-copy delivery only. It does not yet pull APIs, inboxes, object storage, Sheets, email, or webhooks; send mail; render Excel/PDF; infer source-column matches; retry remote operations; or provide multi-host workers. Monthly schedules intentionally allow days 1–28 only. The scheduler uses UTC and must be supervised by the operator's service manager. SQLite is appropriate for one local installation, not a distributed cluster.

## Data and privacy

The application makes no network calls. Paths are containment-checked, config/input/cell/row/column counts are bounded, stored errors redact absolute paths, and the bundled data is explicitly synthetic. Do not put secrets in configuration or CSV files.

## Support development

Donations fund additional development time and can increase production of tested features. See [SUPPORT.md](SUPPORT.md) to request a specific direction with a confirmed public transaction hash. A donation does not buy support, ownership, returns, a deadline, or guaranteed acceptance.

## License

MIT. See [LICENSE](LICENSE).
