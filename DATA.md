# Data dictionary

The pipeline reads order records and a customer reference file. Its target grain is one accepted order per `order_id` per source file. It preserves valid rows in `orders.csv` and writes invalid or duplicate rows to `rejects.jsonl`.

## Order record

| Field | Type / nullability | Source / output | Rule |
|---|---|---|---|
| `order_id` | string, required | Source and output | Stable source key; duplicate within one input file is rejected. |
| `customer_id` | string, required | Source and output | Must match a key in the customer reference file. |
| `order_ts` | ISO-8601 timestamp, required | Source and output | Source must include a timezone; output is normalized to UTC with trailing `Z`. |
| `amount` | decimal with 2 fractional digits, required | Source and output | Finite and nonnegative; normalized to two decimal places. |
| `currency` | string, required | Source and output | Accepted values: USD, EUR, GBP, INR; output is uppercase. |
| `status` | string, required | Source and output | Accepted values: created, paid, shipped, cancelled; output is lowercase. |
| `run_id` | string, output | Pipeline-generated | First 16 lowercase hex characters of the source file SHA-256. |
| `source_file` | string, output | Pipeline-generated | Input file basename for lineage. |
| `source_row` | integer-like string, output | Pipeline-generated | 1-based source record position; CSV header is not counted as a record. |

## Customer reference

| Field | Type / nullability | Rule |
|---|---|---|
| `customer_id` | string, required | Unique reference key used for order referential-integrity checks. |
| `name` | string, optional | Descriptive sample attribute; not used by the pipeline. |

## Run report and rejects

The JSON run report includes `run_id`, `source_file`, ISO-8601 `started_at` and `ended_at`, counts, status aggregates, amount totals by currency, and a reconciliation result. Reject records in `rejects.jsonl` include `source_file`, `source_row`, `reason`, and the raw record when parsing succeeded. Malformed JSONL lines retain source location and parse error; their raw text is not embedded.

Extra source fields are currently tolerated. Missing required fields, invalid values, unknown customers, and duplicate order IDs are rejected.
