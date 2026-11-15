# Streaming Orders Pipeline

A small, dependency-free Python project that ingests large order CSV or JSONL files without materializing the input in memory. It validates records, normalizes output, quarantines rejects with source row lineage, and publishes a deterministic result folder.

## Tech stack

- **Language and runtime:** Python 3.10 or newer.
- **Runtime dependencies:** Python standard library only. The pipeline uses `csv` and `json` for streaming input/output, `sqlite3` for disk-backed duplicate-key tracking, and standard modules for hashing, timestamps, decimal arithmetic, and filesystem operations.
- **Input formats:** CSV and newline-delimited JSON (`.jsonl` / `.ndjson`).
- **Output formats:** normalized CSV, JSONL quarantine records, and a JSON run report.
- **Packaging and CLI:** `pyproject.toml` with setuptools; run with `python -m pipeline.cli` or the installed `orders-pipeline` command.
- **Testing:** pytest, installed with the optional `test` dependency group.
- **Execution environment:** local filesystem; no database server, cloud account, or external service is required. SQLite is used as a temporary local index and removed after a successful run.

## Quick start

Requires Python 3.10+. From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install ".[test]"
python -m pytest
python -m pipeline.cli --input data/sample/orders.csv --customers data/sample/customers.csv --output reports --chunk-size 500
```

The CLI prints a JSON run report. Artifacts are written under `reports/<run_id>/`: `orders.csv`, `rejects.jsonl`, and `run_report.json`. `run_id` is the first 16 hex characters of a SHA-256 over both input file names and contents (orders and customers). Re-running the same pair of inputs replaces that run folder after processing and reconciliation succeed; changing either input produces a new run ID. Work is staged in a sibling directory on the same filesystem before publication. A temporary backup directory allows the previous successful output to be restored if the final rename fails or the process restarts during publication.

Generate a larger deterministic input (100,000 by default):

```powershell
python -m pipeline.generate --output data/generated/orders.csv --customers data/generated/customers.csv --rows 100000 --seed 42
python -m pipeline.cli --input data/generated/orders.csv --customers data/generated/customers.csv --output reports --chunk-size 1000
```

JSONL inputs contain one JSON object per line with the same field names. CSV and JSONL are streamed one record at a time. `chunk_size` sets the transaction commit interval for the on-disk duplicate index and is recorded in the report; processing does not retain a batch of source records. Duplicate-key tracking uses a temporary SQLite index on disk.

## Design document

See [design.md](DESIGN.md) for design choices, trade-offs, and limitations.

## Architecture and behavior

```text
CSV / JSONL source ──stream──> parse + schema/business validation ──> normalized orders.csv
                                      │                              └─> run report + metrics
                                      └─invalid / duplicate─────────> rejects.jsonl
customers.csv ──load reference keys───┘
```

The customer dimension is loaded into a Python set for fast referential checks and is expected to be modest in size. For larger dimensions, replace this with a SQLite/warehouse lookup. The full order input and duplicate-key index are never held in memory. Each reject includes source filename, physical source line number (CSV data starts at line 2 after the header; JSONL starts at line 1), raw record when parseable, and a reason. A fatal processing failure writes `<run_id>.failure.json` and leaves the prior published result in place.

## Naming conventions

- **Stable IDs:** `order_id` is the source-provided, stable order key. `run_id` is deterministic: the first 16 lowercase hexadecimal characters of a SHA-256 over both input file names and contents. The same order and customer files therefore receive the same `run_id`; changing either produces a different ID.
- **snake_case names:** Pipeline data files, logical tables, columns, and report keys use lowercase `snake_case`. The logical target table is named `orders`; its CSV representation is `orders.csv`. Reject details are stored in `rejects.jsonl`.
- **ISO-8601 timestamps:** Input `order_ts` values must include a timezone. The output normalizes them to UTC in ISO-8601 format with a trailing `Z`. Operational `started_at` and `ended_at` report fields use ISO-8601 UTC timestamps.
- **Lineage fields:** Published order rows include `run_id` and `source_file` so records can be traced to their processing run and input file. `source_row` records the source record position. The run report also includes `run_id` and `source_file`; each reject includes `source_file` and `source_row`.

## Data model

The pipeline’s grain is one order per `order_id`; `customer_id` references the customer dimension. Order IDs are unique within one source file. This example treats input as an order snapshot; event history is outside scope. See [data.md](data.md) for the field-level data dictionary and lineage definitions.
Unexpected extra fields are tolerated for forward compatibility and listed in the run report's `unexpected_fields` field. Missing required columns/keys are quarantined (CSV headers missing fields produce schema rejects for each data row). Unknown currencies, missing values, invalid dates, timezone-free timestamps, negative amounts, unknown customers, and duplicate orders are rejected.

## Idempotency, restart, and backfill

The run key derives from input content, so retrying the same file maps to the same directory. Work is first written into a temporary sibling directory and renamed into place after completion. On a fatal failure the previous successful folder is retained and a failure report is emitted. To backfill, provide the desired source file and an output root; different source content naturally gets a distinct run ID. Operationally, archive older runs before storage limits are reached.

## Reconciliation and performance

Before publishing, the pipeline reads the staged target and rejects back incrementally. It verifies `input_rows = output_rows + rejected_rows`, output and reject counts, primary-key uniqueness and membership, full source-row accounting, `run_id` / `source_file` lineage, status counts, and amount totals by currency. The report records the reconciliation result; `parsed_rows` excludes malformed JSONL lines. Tests cover successful reconciliation, a deliberately mismatched aggregate, partial failure and retry, schema changes, and the sample expected summary. `reports/sample_run/` contains a captured run of the starter fixture.

The included generator uses a fixed seed and creates dates in shuffled order; amount zero is valid and exercises a boundary. For throughput comparisons, time runs with different `--chunk-size` settings while keeping the same input and output path; the core path is record-streaming with a bounded working set. One reasonable future optimization for larger source files is bulk committing the temporary SQLite key index (e.g. every N rows), then benchmarking the memory/disk trade-off. This sample favors clear error lineage over maximum throughput.

## Tests and limitations

Tests cover normal output, empty input, malformed JSONL, missing schema, additive schema fields, duplicate key, missing required data, unknown reference, negative amount, zero amount, invalid status, timezone-free timestamp, deterministic generation, reference-file changes, and failure/retry behavior. The sample contains labeled invalid examples; tests reconcile actual results to `data/sample/expected_summary.json`, which is separate from source data.

Limitations: one source file per run; the customer reference keys fit in memory; no cross-file dedupe, currency conversion, or update/event-history semantics; output CSV is not partitioned. The local design uses only Python's standard library at runtime and pytest for tests.






