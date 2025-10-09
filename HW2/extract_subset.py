"""
extract_subset.py

Create a smaller version of the MS MARCO collection by taking
the first N lines from collection.tsv (excluding empty lines).

Usage:
    python extract_subset.py --input data/collection.tsv --output data/marco_small.tsv --limit 10000
"""

import argparse
import os

def extract_subset(input_path, output_path, limit=30000):
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    count = 0

    with open(input_path, "r", encoding="utf-8") as fin, \
         open(output_path, "w", encoding="utf-8") as fout:
        for line in fin:
            if not line.strip():
                continue
            fout.write(line)
            count += 1
            if count >= limit:
                break

    print(f"Wrote {count} lines from {input_path} -> {output_path}")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="collection.tsv", help="Path to full collection.tsv")
    ap.add_argument("--output", default="data/marco_small.tsv", help="Output path for subset")
    ap.add_argument("--limit", type=int, default=30000, help="Number of lines to extract")
    args = ap.parse_args()
    extract_subset(args.input, args.output, args.limit)
