import csv, json
import sqlite3
from collections import Counter
import pytest
from pipeline.core import _reconcile_output
from pathlib import Path
from pipeline.core import PipelineConfig, run_pipeline

def write_customers(path: Path):
    path.write_text("customer_id\nC1\nC2\n", encoding="utf-8")

def test_happy_path_and_idempotent_rerun(tmp_path):
    customers = tmp_path/"customers.csv"; write_customers(customers)
    source = tmp_path/"orders.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\nO1,C1,2025-01-01T00:00:00Z,12.5,usd,PAID\n", encoding="utf-8")
    cfg = PipelineConfig(source, customers, tmp_path/"out", 2)
    first = run_pipeline(cfg); second = run_pipeline(cfg)
    assert first["output_rows"] == 1 and first["rejected_rows"] == 0
    assert first["run_id"] == second["run_id"]
    assert len(list((tmp_path/"out"/first["run_id"]).glob("orders.csv"))) == 1
    with (tmp_path/"out"/first["run_id"]/"orders.csv").open() as f:
        row = next(csv.DictReader(f))
    assert row["amount"] == "12.50" and row["currency"] == "USD"
    assert row["run_id"] == first["run_id"] and row["source_file"] == source.name
    assert first["reconciliation"]["status"] == "passed"
    assert first["reconciliation"]["published_rows_checked"] == 1

def test_empty_input(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"e.csv"; source.write_text("order_id,customer_id,order_ts,amount,currency,status\n", encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    assert result["input_rows"] == result["output_rows"] == 0

def test_rejects_duplicates_null_fk_and_domain_edges(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"rows.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\n"
      "O1,C1,2025-01-01T00:00:00Z,0,USD,created\n"
      "O1,C1,2025-01-02T00:00:00Z,3,USD,paid\n"
      "O2,C9,2025-01-01T00:00:00Z,3,USD,paid\n"
      "O3,C1,2025-01-01T00:00:00Z,-1,USD,paid\n"
      "O4,C1,2025-01-01T00:00:00Z,4,USD,unknown\n"
      "O5,C1,2025-01-01T00:00:00,4,USD,paid\n"
      "O6,,2025-01-01T00:00:00Z,4,USD,paid\n", encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    assert result["output_rows"] == 1 and result["rejected_rows"] == 6
    rejects=[json.loads(x) for x in (tmp_path/"out"/result["run_id"]/"rejects.jsonl").read_text().splitlines()]
    reasons={x["reason"] for x in rejects}
    assert {"duplicate_order_id","unknown_customer","negative_amount","invalid_status","timestamp_timezone_required"} <= reasons

def test_malformed_jsonl_is_quarantined(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"rows.jsonl"
    source.write_text('{"order_id":"O1","customer_id":"C1","order_ts":"2025-01-01T00:00:00Z","amount":"1","currency":"USD","status":"paid"}\n{bad json}\n', encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    assert result["output_rows"] == result["rejected_rows"] == 1
    assert "malformed_json" in (tmp_path/"out"/result["run_id"]/"rejects.jsonl").read_text()

def test_missing_schema_field_rejected(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"rows.jsonl"; source.write_text('{"order_id":"O1"}\n', encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    assert result["rejected_rows"] == 1

def test_reconciliation_fails_on_aggregate_mismatch(tmp_path):
    output = tmp_path/"orders.csv"
    output.write_text("order_id,customer_id,order_ts,amount,currency,status,run_id,source_file,source_row\n"
                      "O1,C1,2025-01-01T00:00:00Z,10.00,USD,paid,R1,orders.csv,2\n", encoding="utf-8")
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE seen (order_id TEXT PRIMARY KEY)")
    db.execute("INSERT INTO seen VALUES ('O1')")
    counts = Counter(input_rows=1, output_rows=1, rejected_rows=0)
    with pytest.raises(RuntimeError, match="status aggregate reconciliation failed"):
        _reconcile_output(output, db, counts, Counter(), Counter(USD=__import__("decimal").Decimal("10.00")), "R1", "orders.csv")
    db.close()

