# Streaming Orders Pipeline

A small, dependency-free Python project that ingests large order CSV or JSONL files without materializing the input in memory. It validates records, normalizes output, quarantines rejects with source row lineage, and publishes a deterministic result folder.

## Quick start

Requires Python 3.10+. From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
python -m pytest
python -m pipeline.cli --input data/sample/orders.csv --customers data/sample/customers.csv --output reports --chunk-size 500
```

The CLI prints a JSON run report. Artifacts are written under `reports/<run_id>/`: `orders.csv`, `rejects.jsonl`, and `run_report.json`. `run_id` is the first 16 hex characters of the source input SHA-256. Re-running identical input atomically replaces the same result folder; a changed input creates a new folder. Keep outputs on a filesystem that supports atomic same-volume rename.

Generate a larger deterministic input (100,000 by default):

```powershell
python -m pipeline.generate --output data/generated/orders.csv --customers data/generated/customers.csv --rows 100000 --seed 42
python -m pipeline.cli --input data/generated/orders.csv --customers data/generated/customers.csv --output reports --chunk-size 1000
```

JSONL inputs contain one JSON object per line with the same field names. CSV and JSONL are streamed one record at a time. `chunk_size` sets the transaction commit interval for the on-disk duplicate index and is recorded in the report; processing does not retain a batch of source records. Duplicate-key tracking uses a temporary SQLite index on disk.

## Architecture and behavior

```text
CSV / JSONL source ──stream──> parse + schema/business validation ──> normalized orders.csv
                                      │                              └─> run report + metrics
                                      └─invalid / duplicate─────────> rejects.jsonl
customers.csv ──load reference keys───┘
```

The customer dimension is loaded into a Python set for fast referential checks and is expected to be modest in size. For larger dimensions, replace this with a SQLite/warehouse lookup. The full order input and duplicate-key index are never held in memory. Each reject includes source filename, 1-based source row (CSV starts at header+1), raw record when parseable, and a reason. A fatal processing failure writes `<run_id>.failure.json` and leaves the prior published result in place.

## Naming conventions

- **Stable IDs:** `order_id` is the source-provided, stable order key. `run_id` is deterministic: the first 16 lowercase hexadecimal characters of the source file SHA-256. Identical input content therefore receives the same `run_id`.
- **snake_case names:** Files, tables, columns, and report keys use lowercase `snake_case`. The logical target table is named `orders`; its CSV representation is `orders.csv`. Reject details are stored in `rejects.jsonl`.
- **ISO-8601 timestamps:** Input `order_ts` values must include a timezone. The output normalizes them to UTC in ISO-8601 format with a trailing `Z`. Operational `started_at` and `ended_at` report fields use ISO-8601 UTC timestamps.
- **Lineage fields:** Published order rows include `run_id` and `source_file` so records can be traced to their processing run and input file. `source_row` records the source record position. The run report also includes `run_id` and `source_file`; each reject includes `source_file` and `source_row`.

## Data model and dictionary

Grain: one order per `order_id`; `customer_id` references `customers.csv`. Order IDs are unique within a source file. Event history is outside this example's scope; this is an order snapshot feed.

| Field | Type / nullability | Rule |
|---|---|---|
| order_id | string, required | Stable source key; duplicate within batch rejected |
| customer_id | string, required | Must exist in customer dimension |
| order_ts | ISO-8601 timestamp, required | Must include timezone; normalized to UTC with `Z` |
| amount | decimal(… ,2), required | Finite, nonnegative; rounded to two decimal places |
| currency | string, required | USD, EUR, GBP, or INR; uppercased |
| status | string, required | created, paid, shipped, or cancelled; lowercased |
| source_file | string, output | Input basename for lineage |
| source_row | integer-like string, output | Source record row/line number |

Unexpected extra fields are tolerated for forward compatibility; missing required columns/keys are quarantined (CSV headers missing fields produce schema rejects for each data row). Unknown currencies, missing values, invalid dates, timezone-free timestamps, negative amounts, unknown customers, and duplicate orders are rejected.

## Idempotency, restart, and backfill

The run key derives from input content, so retrying the same file maps to the same directory. Work is first written into a temporary sibling directory and renamed into place after completion. On a fatal failure the previous successful folder is retained and a failure report is emitted. To backfill, provide the desired source file and an output root; different source content naturally gets a distinct run ID. Operationally, archive older runs before storage limits are reached.

## Reconciliation and performance

The report reconciles `input_rows = output_rows + rejected_rows`; `parsed_rows` excludes malformed JSONL lines. It also records valid-row counts by status and amount totals by currency, plus duration. Use the stable order key to check output uniqueness and independently sum amounts by currency. `reports/sample_run/` contains one captured execution report and output from the starter fixture.

The included generator uses a fixed seed and creates dates in shuffled order; amount zero is valid and exercises a boundary. For throughput comparisons, time runs with different `--chunk-size` settings while keeping the same input and output path; the core path is record-streaming with a bounded working set. One reasonable future optimization for larger source files is bulk committing the temporary SQLite key index (e.g. every N rows), then benchmarking the memory/disk trade-off. This sample favors clear error lineage over maximum throughput.

## Tests and limitations

Tests cover normal output, empty input, malformed JSONL, missing schema, duplicate key, missing required data, unknown reference, negative amount, zero amount, invalid status, timezone-free timestamp, and deterministic generation. The sample contains labeled invalid examples; expected outcomes live in `data/sample/expected_summary.json`, separate from source data.

Limitations: one source file per run; the customer reference keys fit in memory; no cross-file dedupe, currency conversion, or update/event-history semantics; output CSV is not partitioned. The local design uses only Python's standard library at runtime and pytest for tests.


