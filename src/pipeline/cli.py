"""Command line interface."""
import argparse
import json
from pathlib import Path
from .core import PipelineConfig, run_pipeline

def main() -> None:
    parser = argparse.ArgumentParser(description="Stream and validate CSV or JSONL orders")
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--customers", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--chunk-size", type=int, default=1000, help="maximum rows per processing buffer (default: 1000)")
    args = parser.parse_args()
    print(json.dumps(run_pipeline(PipelineConfig(args.input, args.customers, args.output, args.chunk_size)), indent=2))

if __name__ == "__main__":
    main()
