from __future__ import annotations
import csv, hashlib, json, os, shutil, sqlite3, tempfile, time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator, Mapping, Any
OUTPUT_FIELDS = ["order_id", "customer_id", "order_ts", "amount", "currency", "status", "run_id", "source_file", "source_row"]
REQUIRED = {"order_id", "customer_id", "order_ts", "amount", "currency", "status"}
ALLOWED_STATUS = {"created", "paid", "shipped", "cancelled"}
CURRENCY = {"USD", "EUR", "GBP", "INR"}
@dataclass(frozen=True)
class PipelineConfig:
    input_path: Path
    customers_path: Path
    output_dir: Path
    chunk_size: int = 1000

def _load_customers(path: Path) -> set[str]:
    known: set[str] = set()
    with path.open("r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames or "customer_id" not in reader.fieldnames:
            raise ValueError("customers file must have a customer_id column")
        for row in reader:
            value = (row.get("customer_id") or "").strip()
            if value: known.add(value)
    return known

def _iter_records(path: Path) -> Iterator[tuple[int, dict[str, Any] | None, str | None]]:
    if path.suffix.lower() == ".csv":
        with path.open("r", newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames: return
            for row_num, row in enumerate(reader, start=2): yield row_num, row, None
    elif path.suffix.lower() in {".jsonl", ".ndjson"}:
        with path.open("r", encoding="utf-8-sig") as f:
            for row_num, line in enumerate(f, start=1):
                try:
                    value = json.loads(line)
                    if not isinstance(value, dict): raise ValueError("each JSONL line must be an object")
                    yield row_num, value, None
                except (json.JSONDecodeError, ValueError) as exc: yield row_num, None, f"malformed_json: {exc}"
    else: raise ValueError("input must be .csv, .jsonl, or .ndjson")

def _normalize(row: Mapping[str, Any], customers: set[str]) -> tuple[dict[str, str] | None, str | None]:
    missing_columns = REQUIRED - set(row.keys())
    if missing_columns: return None, "schema_missing_columns:" + ",".join(sorted(missing_columns))
    values = {key: "" if row.get(key) is None else str(row.get(key)).strip() for key in REQUIRED}
    missing = sorted(k for k in REQUIRED if not values[k])
    if missing: return None, "missing_required:" + ",".join(missing)
    if len(values["order_id"]) > 100 or len(values["customer_id"]) > 100: return None, "identifier_too_long"
    if values["customer_id"] not in customers: return None, "unknown_customer"
    try:
        amount = Decimal(values["amount"])
        if not amount.is_finite(): return None, "invalid_amount"
        if amount < 0: return None, "negative_amount"
        amount = amount.quantize(Decimal("0.01"))
    except (InvalidOperation, ValueError): return None, "invalid_amount"
    try:
        parsed = datetime.fromisoformat(values["order_ts"].replace("Z", "+00:00"))
        if parsed.tzinfo is None: return None, "timestamp_timezone_required"
        timestamp = parsed.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except ValueError: return None, "invalid_timestamp"
    if values["currency"].upper() not in CURRENCY: return None, "unsupported_currency"
    status = values["status"].lower()
    if status not in ALLOWED_STATUS: return None, "invalid_status"
    return {"order_id": values["order_id"], "customer_id": values["customer_id"], "order_ts": timestamp, "amount": f"{amount:.2f}", "currency": values["currency"].upper(), "status": status}, None

def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""): digest.update(block)
    return digest.hexdigest()

def _run_id(source: Path, customers_path: Path) -> str:
    """Derive a stable run key from both data inputs, including the reference dimension."""
    digest = hashlib.sha256()
    for label, path in ((b"source\0", source), (b"customers\0", customers_path)):
        digest.update(label)
        digest.update(path.name.encode("utf-8"))
        digest.update(b"\0")
        with path.open("rb") as f:
            for block in iter(lambda: f.read(1024 * 1024), b""):
                digest.update(block)
    return digest.hexdigest()[:16]

def run_pipeline(config: PipelineConfig) -> dict[str, Any]:
    source, customers_path, output = Path(config.input_path), Path(config.customers_path), Path(config.output_dir)
    if config.chunk_size <= 0: raise ValueError("chunk_size must be positive")
    customers = _load_customers(customers_path)
    run_id = _run_id(source, customers_path)
    started, timer = datetime.now(timezone.utc), time.perf_counter()
    output.mkdir(parents=True, exist_ok=True)
    final_dir = output / run_id
    backup_dir = output / f".{run_id}.previous"
    if backup_dir.exists():
        if final_dir.exists():
            shutil.rmtree(backup_dir)
        else:
            os.replace(backup_dir, final_dir)
    staging = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=output))
    counts, status_counts, amount_totals = Counter(input_rows=0, parsed_rows=0, output_rows=0, rejected_rows=0), Counter(), Counter()
    db = None
    unexpected_fields: set[str] = set()
    try:
        # SQLite keeps duplicate-key state on disk, so memory does not grow with input row count.
        db = sqlite3.connect(staging / "seen.sqlite")
        db.execute("CREATE TABLE seen (order_id TEXT PRIMARY KEY)")
        db.execute("CREATE TABLE published (order_id TEXT PRIMARY KEY)")
        db.execute("CREATE TABLE source_rows (source_row INTEGER PRIMARY KEY)")
        with (staging / "orders.csv").open("w", newline="", encoding="utf-8") as valid_file, (staging / "rejects.jsonl").open("w", encoding="utf-8") as reject_file:
            writer = csv.DictWriter(valid_file, fieldnames=OUTPUT_FIELDS); writer.writeheader()
            for row_num, raw, parse_error in _iter_records(source):
                counts["input_rows"] += 1
                if counts["input_rows"] % config.chunk_size == 0: db.commit()
                if parse_error:
                    _write_reject(reject_file, source, row_num, None, parse_error); counts["rejected_rows"] += 1; continue
                assert raw is not None
                counts["parsed_rows"] += 1
                unexpected_fields.update(
                    key if isinstance(key, str) and key else "<extra_values>"
                    for key in (set(raw) - REQUIRED)
                )
                normalized, error = _normalize(raw, customers)
                if not error and normalized:
                    try: db.execute("INSERT INTO seen VALUES (?)", (normalized["order_id"],))
                    except sqlite3.IntegrityError: error = "duplicate_order_id"
                if error or not normalized:
                    _write_reject(reject_file, source, row_num, raw, error or "invalid_record"); counts["rejected_rows"] += 1; continue
                normalized.update(run_id=run_id, source_file=source.name, source_row=str(row_num)); writer.writerow(normalized)
                counts["output_rows"] += 1; status_counts[normalized["status"]] += 1
                amount_totals[normalized["currency"]] += Decimal(normalized["amount"])
        db.commit()
        reconciliation = _reconcile_output(staging / "orders.csv", staging / "rejects.jsonl", db,
                                            counts, status_counts, amount_totals, run_id, source.name)
        db.close()
        db = None
        # SQLite is an internal scratch index, not part of the published dataset.
        (staging / "seen.sqlite").unlink(missing_ok=True)
        ended = datetime.now(timezone.utc)
        report = {"run_id": run_id, "status": "success", "source_file": source.name,
          "input_sha256": _sha256(source), "customers_sha256": _sha256(customers_path),
          "started_at": started.isoformat(), "ended_at": ended.isoformat(), "duration_seconds": round(time.perf_counter()-timer, 6),
          "chunk_size": config.chunk_size, **dict(counts), "distinct_customers_loaded": len(customers),
          "status_counts": dict(sorted(status_counts.items())), "amount_totals_by_currency": {k:f"{v:.2f}" for k,v in sorted(amount_totals.items())},
          "unexpected_fields": sorted(unexpected_fields), "reconciliation": reconciliation, "failure_reason": None}
        (staging / "run_report.json").write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
        if final_dir.exists():
            os.replace(final_dir, backup_dir)
        try:
            os.replace(staging, final_dir)
        except Exception:
            if backup_dir.exists() and not final_dir.exists():
                os.replace(backup_dir, final_dir)
            raise
        if backup_dir.exists():
            shutil.rmtree(backup_dir)
        (output / f"{run_id}.failure.json").unlink(missing_ok=True)
        return report
    except Exception as exc:
        if db is not None:
            db.close()
        failure = {"run_id":run_id,"status":"failed","source_file":source.name,"started_at":started.isoformat(),"ended_at":datetime.now(timezone.utc).isoformat(),"duration_seconds":round(time.perf_counter()-timer,6),**dict(counts),"failure_reason":str(exc)}
        (output/f"{run_id}.failure.json").write_text(json.dumps(failure,indent=2)+"\n",encoding="utf-8")
        raise
    finally:
        if staging.exists(): shutil.rmtree(staging, ignore_errors=True)


def _reconcile_output(path: Path, rejects_path: Path, db: sqlite3.Connection, counts: Counter,
                      expected_status: Counter, expected_amounts: Counter,
                      run_id: str, source_file: str) -> dict[str, Any]:
    """Read staged outputs back incrementally and verify counts, keys, lineage, and aggregates."""
    actual_count = 0
    status_counts: Counter = Counter()
    amount_totals: Counter = Counter()
    with path.open("r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            actual_count += 1
            if row["run_id"] != run_id or row["source_file"] != source_file:
                raise RuntimeError("output lineage reconciliation failed")
            try:
                db.execute("INSERT INTO published VALUES (?)", (row["order_id"],))
                db.execute("INSERT INTO source_rows VALUES (?)", (int(row["source_row"]),))
            except (sqlite3.IntegrityError, ValueError) as exc:
                raise RuntimeError("output key or source-row uniqueness reconciliation failed") from exc
            status_counts[row["status"]] += 1
            amount_totals[row["currency"]] += Decimal(row["amount"])
    reject_count = 0
    with rejects_path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                reject = json.loads(line)
                if reject["source_file"] != source_file or not reject["reason"]:
                    raise ValueError("reject lineage or reason is missing")
                db.execute("INSERT INTO source_rows VALUES (?)", (int(reject["source_row"]),))
            except (json.JSONDecodeError, KeyError, TypeError, ValueError, sqlite3.IntegrityError) as exc:
                raise RuntimeError("reject count or source-row reconciliation failed") from exc
            reject_count += 1
    unique_key_count = db.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    published_key_count = db.execute("SELECT COUNT(*) FROM published").fetchone()[0]
    matched_key_count = db.execute("SELECT COUNT(*) FROM seen s JOIN published p USING (order_id)").fetchone()[0]
    source_row_count = db.execute("SELECT COUNT(*) FROM source_rows").fetchone()[0]
    expected_totals = {key: f"{value:.2f}" for key, value in sorted(expected_amounts.items())}
    actual_totals = {key: f"{value:.2f}" for key, value in sorted(amount_totals.items())}
    if counts["input_rows"] != counts["output_rows"] + counts["rejected_rows"]:
        raise RuntimeError("source-to-target count reconciliation failed")
    if (actual_count != counts["output_rows"] or unique_key_count != actual_count or
            published_key_count != actual_count or matched_key_count != actual_count):
        raise RuntimeError("output row count or primary-key reconciliation failed")
    if reject_count != counts["rejected_rows"] or source_row_count != counts["input_rows"]:
        raise RuntimeError("reject count or source-row reconciliation failed")
    if dict(sorted(status_counts.items())) != dict(sorted(expected_status.items())):
        raise RuntimeError("status aggregate reconciliation failed")
    if actual_totals != expected_totals:
        raise RuntimeError("amount aggregate reconciliation failed")
    return {"status": "passed", "input_equals_output_plus_rejects": True,
            "published_rows_match_report": True, "published_keys_unique": True,
            "reject_rows_match_report": True, "all_source_rows_accounted_for": True,
            "status_counts_match": True, "amount_totals_match": True,
            "published_rows_checked": actual_count}
def _write_reject(file, source: Path, row_num: int, raw: Mapping[str, Any] | None, reason: str) -> None:
    file.write(json.dumps({"source_file":source.name,"source_row":row_num,"reason":reason,"record":raw},ensure_ascii=False)+"\n")






