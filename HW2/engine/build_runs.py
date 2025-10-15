# engine/build_runs.py
"""
Build sorted TSV runs from a large TSV corpus (batching driver).

Pipeline per batch:
  1) Parse texts into tokens (using Parser).
  2) Build an in-memory postings map (using Indexer).
  3) Flatten to rows (term, docid, tf), sort by (term, docid).
  4) Write a TSV run via RunWriter.

After all batches:
  - Persist doc_lengths.pkl once (for BM25).
  - You can then merge the runs into the final blocked index with engine.merger.

This driver does NOT write the final postings/lexicon. It only produces
uncompressed "intermediate postings": globally-sorted TSV runs.
"""

from __future__ import annotations
import os
import argparse
from typing import Dict, List, Tuple

from engine.parser import Parser
from engine.indexer import Indexer
from engine.runio import RunWriter
from engine.utils import write_doc_lengths 
from engine.paths import DOC_LENGTHS_PATH   # global path for doc lengths


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def iter_tsv_lines(tsv_path: str):
    """Stream lines from a TSV file as raw strings (no header expected)."""
    with open(tsv_path, "r", encoding="utf8") as f:
        for line in f:
            # Keep exactly one trailing newline stripped
            yield line.rstrip("\n")


def flush_batch_to_run(
    batch_docs: Dict[int, List[str]],
    run_path: str,
) -> int:
    """
    Build postings for a batch and write a single sorted run file.

    Uses your existing RunWriter.write_from_index(postings), where
    postings is a dict[str, dict[int, int]] mapping term -> {docid: tf}.
    Returns the number of (term, docid, tf) rows written to the run.
    """
    # Build an in-memory inverted index for this batch
    indexer = Indexer()
    indexer.build_inverted_index(batch_docs)

    # Count rows for logging
    n_rows = sum(len(plist) for plist in indexer.index.values())

    # RunWriter.write_from_index already sorts by (term, docid)
    postings = {term: dict(plist) for term, plist in indexer.index.items()}
    with RunWriter(run_path) as w:
        w.write_from_index(postings)

    return n_rows


def build_runs(
    input_tsv: str,
    outdir: str,
    batch_size: int = 100_000,
    start_docid: int = 0,
    write_lengths: bool = True,
) -> List[str]:
    """
    Read a large TSV corpus and produce multiple globally-sorted runs.

    Parameters
    ----------
    input_tsv : str
        Path to the large TSV file (e.g., data/marco_medium.tsv).
        We assume the TEXT is in the last column of each line.
    outdir : str
        Directory where run_*.tsv will be written.
    batch_size : int
        Number of documents per run (tune for memory).
    start_docid : int
        Starting docid for the first document.
    write_lengths : bool
        If True, persist DOC_LENGTHS_PATH at the end.

    Returns
    -------
    List[str]
        Absolute paths to the run files created.
    """
    ensure_dir(outdir)
    parser = Parser()

    run_paths: List[str] = []
    batch_docs: Dict[int, List[str]] = {}   # docid -> tokens
    doc_lengths: Dict[int, int] = {}        # docid -> length

    docid = start_docid
    batch_idx = 0

    def flush():
        nonlocal batch_docs, batch_idx
        if not batch_docs:
            return
        run_path = os.path.join(outdir, f"run_{batch_idx:06d}.tsv")
        n_rows = flush_batch_to_run(batch_docs, run_path)
        run_paths.append(run_path)
        print(f"[BuildRuns] Wrote {run_path}  rows={n_rows}  docs={len(batch_docs)}")
        batch_docs.clear()
        batch_idx += 1

    # Stream the input TSV and accumulate a batch of docs
    for line in iter_tsv_lines(input_tsv):
        if not line.strip():
            continue
        parts = line.split("\t")
        text = parts[-1]             # last column as text
        tokens = parser.tokenize(text)
        batch_docs[docid] = tokens
        doc_lengths[docid] = len(tokens)
        docid += 1

        if len(batch_docs) >= batch_size:
            flush()

    # Flush the final (possibly partial) batch
    flush()

    # Persist document lengths once
    if write_lengths:
        write_doc_lengths(doc_lengths, DOC_LENGTHS_PATH)
        print(f"[BuildRuns] Doc lengths saved: {DOC_LENGTHS_PATH} (N={len(doc_lengths)})")

    print(f"[BuildRuns] Total runs: {len(run_paths)}")
    return run_paths


def main():
    ap = argparse.ArgumentParser(description="Build sorted TSV runs from a large TSV corpus.")
    ap.add_argument("--input", required=True, help="Input TSV file (e.g., data/marco_medium.tsv)")
    ap.add_argument("--outdir", default="data/runs", help="Output directory for run_*.tsv")
    ap.add_argument("--batch-size", type=int, default=100_000, help="Docs per run (tune for memory)")
    ap.add_argument("--start-docid", type=int, default=0, help="Starting docid (default: 0)")
    ap.add_argument("--no-lengths", action="store_true", help="Do not write doc_lengths.pkl")
    args = ap.parse_args()

    build_runs(
        input_tsv=args.input,
        outdir=args.outdir,
        batch_size=args.batch_size,
        start_docid=args.start_docid,
        write_lengths=not args.no_lengths,
    )


if __name__ == "__main__":
    main()
