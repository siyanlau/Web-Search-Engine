# engine/tools/build_doc_lengths_from_runs.py
"""
Rebuild doc_lengths.pkl directly from intermediate runs (TSV or RUN1).
For each (term, docid, tf) we accumulate: doc_lengths[docid] += tf.

Usage:
  python -m engine.tools.build_doc_lengths_from_runs data/runs/*.run
  # or if runs are TSV:
  python -m engine.tools.build_doc_lengths_from_runs data/runs/*.tsv
"""

from __future__ import annotations
import argparse, glob, os, sys, pickle
from collections import defaultdict
from heapq import merge as kmerge
from typing import List, Sequence, Iterator, Tuple

# re-use your existing readers and default output path
from engine.merger import open_run_reader
from engine.paths import DOC_LENGTHS_PATH  # typically "data/doc_lengths.pkl"

def _expand_globs(paths: Sequence[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        if any(ch in p for ch in "*?[]"):
            out.extend(sorted(glob.glob(p)))
        else:
            out.append(p)
    return out

def build_doc_lengths(run_paths: Sequence[str]) -> dict[int, int]:
    readers = [open_run_reader(p) for p in run_paths]
    stream: Iterator[Tuple[str, int, int]] = kmerge(*(iter(r) for r in readers), key=lambda x: (x[0], x[1]))

    doc_lengths: defaultdict[int, int] = defaultdict(int)
    consumed = 0
    for _, docid, tf in stream:
        doc_lengths[docid] += tf
        consumed += 1
        if consumed % 5_000_000 == 0:
            print(f"[doclen] consumed={consumed:,}  unique_docs={len(doc_lengths):,}", file=sys.stderr)

    # best-effort close
    for r in readers:
        close = getattr(r, "close", None)
        if callable(close):
            try: close()
            except Exception: pass

    return dict(doc_lengths)

def main():
    ap = argparse.ArgumentParser(description="Rebuild doc_lengths.pkl from runs.")
    ap.add_argument("runs", nargs="+", help="Input runs (glob or list). TSV or RUN1.")
    ap.add_argument("--out", default=DOC_LENGTHS_PATH, help="Output pickle path for doc_lengths.")
    args = ap.parse_args()

    paths = _expand_globs(args.runs)
    if not paths:
        print("No input runs.", file=sys.stderr); sys.exit(2)

    print(f"[doclen] scanning {len(paths)} runs ...", file=sys.stderr)
    d = build_doc_lengths(paths)
    with open(args.out, "wb") as f:
        pickle.dump(d, f)
    print(f"[doclen] wrote {args.out} | docs={len(d):,}", file=sys.stderr)

if __name__ == "__main__":
    main()
