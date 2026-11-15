"""Fixed-seed, scalable CSV fixture generator."""
import argparse
import csv
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

STATUSES = ["created", "paid", "shipped", "cancelled"]
CURRENCIES = ["USD", "EUR", "GBP", "INR"]

def generate(output: Path, customers_path: Path, rows: int, seed: int = 42) -> None:
    if rows < 0:
        raise ValueError("rows must be non-negative")
    rng = random.Random(seed)
    customer_ids = [f"C{i:06d}" for i in range(max(1, min(rows // 10 + 1, 10000)))]
    customers_path.parent.mkdir(parents=True, exist_ok=True)
    with customers_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f); writer.writerow(["customer_id"])
        writer.writerows([[x] for x in customer_ids])
    output.parent.mkdir(parents=True, exist_ok=True)
    base = datetime(2025, 1, 1, tzinfo=timezone.utc)
    with output.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "customer_id", "order_ts", "amount", "currency", "status"])
        for i in range(rows):
            # deterministic shuffled days exercise out-of-order arrival safely
            day = rng.randrange(366)
            amount = "0.00" if i % 97 == 0 else f"{rng.uniform(0, 5000):.2f}"
            writer.writerow([f"O{i:09d}", rng.choice(customer_ids), (base + timedelta(days=day, minutes=i % 1440)).isoformat().replace("+00:00", "Z"), amount, rng.choice(CURRENCIES), rng.choice(STATUSES)])

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--output", type=Path, required=True); p.add_argument("--customers", type=Path, required=True); p.add_argument("--rows", type=int, default=100000); p.add_argument("--seed", type=int, default=42)
    a = p.parse_args(); generate(a.output, a.customers, a.rows, a.seed)
