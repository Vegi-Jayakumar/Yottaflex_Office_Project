import csv, json
import sqlite3
from collections import Counter
from decimal import Decimal
from pathlib import Path
import pytest
from pipeline import core
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
    assert second["reconciliation"] == first["reconciliation"]

def test_empty_input(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"e.csv"; source.write_text("order_id,customer_id,order_ts,amount,currency,status\n", encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    assert result["input_rows"] == result["output_rows"] == 0

def test_out_of_order_timestamps_are_kept_in_source_order(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"late_orders.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\n"
                      "O-LATE,C1,2025-01-03T00:00:00Z,2,USD,paid\n"
                      "O-EARLY,C2,2025-01-01T00:00:00Z,3,USD,created\n", encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    with (tmp_path/"out"/result["run_id"] / "orders.csv").open(newline="") as f:
        rows=list(csv.DictReader(f))
    assert result["output_rows"] == 2 and result["rejected_rows"] == 0
    assert [row["order_id"] for row in rows] == ["O-LATE", "O-EARLY"]
    assert rows[0]["order_ts"] > rows[1]["order_ts"]

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

def test_schema_change_is_reported_and_missing_csv_column_rejected(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"rows.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status,region\n"
                      "O1,C1,2025-01-01T00:00:00Z,2,USD,paid,west\n"
                      "O2,C2,2025-01-02T00:00:00Z,3,USD,created,east,unheaded\n", encoding="utf-8")
    result=run_pipeline(PipelineConfig(source,customers,tmp_path/"out1"))
    assert result["output_rows"] == 2
    assert result["unexpected_fields"] == ["<extra_values>", "region"]
    missing=tmp_path/"missing.csv"
    missing.write_text("order_id,customer_id,order_ts,amount,status\n"
                       "O2,C1,2025-01-01T00:00:00Z,2,paid\n", encoding="utf-8")
    rejected=run_pipeline(PipelineConfig(missing,customers,tmp_path/"out2"))
    assert rejected["output_rows"] == 0 and rejected["rejected_rows"] == 1
    reject=json.loads((tmp_path/"out2"/rejected["run_id"]/"rejects.jsonl").read_text())
    assert reject["reason"] == "schema_missing_columns:currency"

def test_run_id_changes_when_customer_reference_changes(tmp_path):
    customers=tmp_path/"customers.csv"; write_customers(customers)
    source=tmp_path/"orders.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\n"
                      "O1,C1,2025-01-01T00:00:00Z,2,USD,paid\n", encoding="utf-8")
    first=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    customers.write_text("customer_id\nC1\n", encoding="utf-8")
    second=run_pipeline(PipelineConfig(source,customers,tmp_path/"out"))
    assert first["run_id"] != second["run_id"]

def test_partial_failure_preserves_previous_output_and_retry_clears_failure(tmp_path, monkeypatch):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"orders.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\n"
                      "O1,C1,2025-01-01T00:00:00Z,2,USD,paid\n"
                      "O2,C2,2025-01-02T00:00:00Z,3,USD,created\n", encoding="utf-8")
    cfg=PipelineConfig(source,customers,tmp_path/"out")
    successful=run_pipeline(cfg)
    result_dir=tmp_path/"out"/successful["run_id"]
    prior=(result_dir/"orders.csv").read_bytes()
    original_iter=core._iter_records
    def broken_iter(path):
        yield from original_iter(path)
        raise OSError("simulated interrupted read")
    monkeypatch.setattr(core,"_iter_records",broken_iter)
    with pytest.raises(OSError,match="simulated interrupted read"):
        run_pipeline(cfg)
    assert (result_dir/"orders.csv").read_bytes() == prior
    failure=tmp_path/"out"/f"{successful['run_id']}.failure.json"
    assert json.loads(failure.read_text())["status"] == "failed"
    monkeypatch.setattr(core,"_iter_records",original_iter)
    retried=run_pipeline(cfg)
    assert retried["status"] == "success"
    assert not failure.exists()

def test_publication_rename_failure_restores_previous_result(tmp_path, monkeypatch):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"orders.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\n"
                      "O1,C1,2025-01-01T00:00:00Z,2,USD,paid\n", encoding="utf-8")
    cfg=PipelineConfig(source,customers,tmp_path/"out")
    successful=run_pipeline(cfg)
    final=tmp_path/"out"/successful["run_id"]
    prior=(final/"orders.csv").read_bytes()
    original_replace=core.os.replace
    failed=[False]
    def fail_stage_once(src,dst):
        if Path(dst) == final and Path(src) != final and not failed[0]:
            failed[0]=True
            raise OSError("simulated publish rename failure")
        return original_replace(src,dst)
    monkeypatch.setattr(core.os,"replace",fail_stage_once)
    with pytest.raises(OSError,match="simulated publish rename failure"):
        run_pipeline(cfg)
    assert (final/"orders.csv").read_bytes() == prior
    assert not (tmp_path/"out"/f".{successful['run_id']}.previous").exists()

def test_restart_restores_backup_directory(tmp_path):
    customers=tmp_path/"c.csv"; write_customers(customers)
    source=tmp_path/"orders.csv"
    source.write_text("order_id,customer_id,order_ts,amount,currency,status\n"
                      "O1,C1,2025-01-01T00:00:00Z,2,USD,paid\n", encoding="utf-8")
    cfg=PipelineConfig(source,customers,tmp_path/"out")
    first=run_pipeline(cfg)
    output=tmp_path/"out"; final=output/first["run_id"]
    backup=output/f".{first['run_id']}.previous"
    final.rename(backup)
    result=run_pipeline(cfg)
    assert result["status"] == "success"
    assert (final/"run_report.json").exists()
    assert not backup.exists()

def test_sample_expected_summary_matches_actual_run(tmp_path):
    project=Path(__file__).parents[1]
    expected=json.loads((project/"data/sample/expected_summary.json").read_text())
    result=run_pipeline(PipelineConfig(project/"data/sample/orders.csv",
                                       project/"data/sample/customers.csv",tmp_path/"out"))
    assert result["input_rows"] == expected["expected_input_rows"]
    assert result["output_rows"] == expected["expected_output_rows"]
    assert result["rejected_rows"] == expected["expected_rejected_rows"]
    assert result["amount_totals_by_currency"] == expected["expected_amount_totals_by_currency"]
    rejects=[json.loads(line) for line in (tmp_path/"out"/result["run_id"]/"rejects.jsonl").read_text().splitlines()]
    assert dict(Counter(item["reason"] for item in rejects)) == expected["expected_reject_reasons"]

def test_reconciliation_fails_on_aggregate_mismatch(tmp_path):
    output = tmp_path/"orders.csv"
    output.write_text("order_id,customer_id,order_ts,amount,currency,status,run_id,source_file,source_row\n"
                      "O1,C1,2025-01-01T00:00:00Z,10.00,USD,paid,R1,orders.csv,2\n", encoding="utf-8")
    db = sqlite3.connect(":memory:")
    db.execute("CREATE TABLE seen (order_id TEXT PRIMARY KEY)")
    db.execute("CREATE TABLE published (order_id TEXT PRIMARY KEY)")
    db.execute("CREATE TABLE source_rows (source_row INTEGER PRIMARY KEY)")
    db.execute("INSERT INTO seen VALUES ('O1')")
    counts = Counter(input_rows=1, output_rows=1, rejected_rows=0)
    rejects=tmp_path/"rejects.jsonl"; rejects.write_text("",encoding="utf-8")
    with pytest.raises(RuntimeError, match="status aggregate reconciliation failed"):
        core._reconcile_output(output, rejects, db, counts, Counter(), Counter(USD=Decimal("10.00")), "R1", "orders.csv")
    db.close()

