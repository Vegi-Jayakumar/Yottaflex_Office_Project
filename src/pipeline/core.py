from __future__ import annotations
import csv, hashlib, json, os, shutil, sqlite3, tempfile, time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Iterator, Mapping, Any
OUTPUT_FIELDS = ["order_id", "customer_id", "order_ts", "amount", "currency", "status", "source_file", "source_row"]
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

def run_pipeline(config: PipelineConfig) -> dict[str, Any]:
    source, customers_path, output = Path(config.input_path), Path(config.customers_path), Path(config.output_dir)
    if config.chunk_size <= 0: raise ValueError("chunk_size must be positive")
    customers = _load_customers(customers_path)
    run_id = _sha256(source)[:16]
    started, timer = datetime.now(timezone.utc), time.perf_counter()
    output.mkdir(parents=True, exist_ok=True)
    final_dir = output / run_id
    staging = Path(tempfile.mkdtemp(prefix=f".{run_id}.", dir=output))
    counts, status_counts, amount_totals = Counter(input_rows=0, parsed_rows=0, output_rows=0, rejected_rows=0), Counter(), Counter()
    try:
        # SQLite keeps duplicate-key state on disk, so memory does not grow with input row count.
        db = sqlite3.connect(staging / "seen.sqlite")
        db.execute("CREATE TABLE seen (order_id TEXT PRIMARY KEY)")
        with (staging / "orders.csv").open("w", newline="", encoding="utf-8") as valid_file, (staging / "rejects.jsonl").open("w", encoding="utf-8") as reject_file:
            writer = csv.DictWriter(valid_file, fieldnames=OUTPUT_FIELDS); writer.writeheader()
            for row_num, raw, parse_error in _iter_records(source):
                counts["input_rows"] += 1
                if counts["input_rows"] % config.chunk_size == 0: db.commit()
                if parse_error:
                    _write_reject(reject_file, source, row_num, None, parse_error); counts["rejected_rows"] += 1; continue
                assert raw is not None
                counts["parsed_rows"] += 1
                normalized, error = _normalize(raw, customers)
                if not error and normalized:
                    try: db.execute("INSERT INTO seen VALUES (?)", (normalized["order_id"],))
                    except sqlite3.IntegrityError: error = "duplicate_order_id"
                if error or not normalized:
                    _write_reject(reject_file, source, row_num, raw, error or "invalid_record"); counts["rejected_rows"] += 1; continue
                normalized.update(source_file=source.name, source_row=str(row_num)); writer.writerow(normalized)
                counts["output_rows"] += 1; status_counts[normalized["status"]] += 1
                amount_totals[normalized["currency"]] += Decimal(normalized["amount"])
        db.commit()
        db.close()
        # SQLite is an internal scratch index, not part of the published dataset.
        (staging / "seen.sqlite").unlink(missing_ok=True)
        ended = datetime.now(timezone.utc)
        report = {"run_id": run_id, "status": "success", "source_file": source.name, "input_sha256": _sha256(source),
          "started_at": started.isoformat(), "ended_at": ended.isoformat(), "duration_seconds": round(time.perf_counter()-timer, 6),
          "chunk_size": config.chunk_size, **dict(counts), "distinct_customers_loaded": len(customers),
          "status_counts": dict(sorted(status_counts.items())), "amount_totals_by_currency": {k:f"{v:.2f}" for k,v in sorted(amount_totals.items())}, "failure_reason": None}
        (staging / "run_report.json").write_text(json.dumps(report, indent=2)+"\n", encoding="utf-8")
        if final_dir.exists(): shutil.rmtree(final_dir)
        os.replace(staging, final_dir)
        return report
    except Exception as exc:
        failure = {"run_id":run_id,"status":"failed","source_file":source.name,"started_at":started.isoformat(),"ended_at":datetime.now(timezone.utc).isoformat(),"duration_seconds":round(time.perf_counter()-timer,6),**dict(counts),"failure_reason":str(exc)}
        (output/f"{run_id}.failure.json").write_text(json.dumps(failure,indent=2)+"\n",encoding="utf-8")
        raise
    finally:
        if staging.exists(): shutil.rmtree(staging, ignore_errors=True)

def _write_reject(file, source: Path, row_num: int, raw: Mapping[str, Any] | None, reason: str) -> None:
    file.write(json.dumps({"source_file":source.name,"source_row":row_num,"reason":reason,"record":raw},ensure_ascii=False)+"\n")




