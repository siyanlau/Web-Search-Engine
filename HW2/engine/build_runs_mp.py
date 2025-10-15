# engine/build_runs_mp.py
"""
Build sorted TSV runs in parallel (multiprocessing).

Why processes, not threads?
- Tokenization (regex + ftfy) and in-memory indexing are CPU-bound.
- The Python GIL prevents true parallelism with threads for CPU-bound work.
- Processes sidestep the GIL and scale across cores.

Design:
- Main process streams the big TSV, packs batches (doc_count = batch_size).
- Each batch is submitted to a worker process:
    worker(batch_lines, start_docid, run_path) -> (n_docs, n_rows, doclen_pairs)
  where:
    - batch_lines: list[str], raw TSV lines (read-only)
    - start_docid: int, docid assigned to the first line in this batch
    - run_path: where to write run_XXXXXX.tsv (via RunWriter.write_from_index)

- Each worker:
    Parser.tokenize() -> Indexer.build_inverted_index(docs_in_batch)
    -> RunWriter.write_from_index(postings)
    -> return doc_lengths for the docid range

- Main process merges all per-batch doclen_pairs and writes DOC_LENGTHS_PATH once.

Outputs:
- data/runs/run_*.tsv   (uncompressed intermediate postings)
- data/doc_lengths.pkl  (consistent with docIDs used in runs)

How to use:
python -m engine.build_runs_mp --input data/marco_medium.tsv --outdir data/runs --batch-size 50000 --workers 4

After this, run your existing merger:
    python -m engine.merger data/runs/run_*.tsv
"""

from __future__ import annotations
import os
import argparse
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Dict, List, Tuple

from engine.parser import Parser
from engine.indexer import Indexer
from engine.runio import RunWriter
from engine.utils import write_doc_lengths
from engine.paths import DOC_LENGTHS_PATH


def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def read_tsv_stream(tsv_path: str):
    """Yield raw lines (stripped trailing '\\n') from a TSV file."""
    with open(tsv_path, "r", encoding="utf8", errors="ignore") as f:
        for line in f:
            yield line.rstrip("\n")


def _worker_build_run(batch_lines: List[str], start_docid: int, run_path: str) -> Tuple[int, int, List[Tuple[int, int]]]:
    """
    Worker process:
    - tokenizes each line's last column
    - builds in-memory postings (term -> {docid: tf})
    - writes a sorted run via RunWriter.write_from_index()
    Returns:
        (n_docs, n_rows, doclen_pairs[(docid, length), ...])
    """
    parser = Parser()

    # Build docs of this batch: docid -> tokens
    docs: Dict[int, List[str]] = {}
    doclen_pairs: List[Tuple[int, int]] = []

    docid = start_docid
    for raw in batch_lines:
        if not raw.strip():
            docid += 1
            continue
        parts = raw.split("\t")
        text = parts[-1]  # last column as text
        toks = parser.tokenize(text)
        docs[docid] = toks
        doclen_pairs.append((docid, len(toks)))
        docid += 1

    # Build small inverted index
    indexer = Indexer()
    indexer.build_inverted_index(docs)

    # RunWriter knows how to sort (term, then docid)
    postings = {term: dict(plist) for term, plist in indexer.index.items()}

    with RunWriter(run_path) as w:
        w.write_from_index(postings)

    n_docs = len(docs)
    n_rows = sum(len(plist) for plist in indexer.index.values())
    return n_docs, n_rows, doclen_pairs


def build_runs_mp(
    input_tsv: str,
    outdir: str,
    batch_size: int = 200_000,
    start_docid: int = 0,
    max_workers: int | None = None,
) -> List[str]:
    """
    Parallel run builder. Adjust batch_size to balance memory vs #runs.

    - Larger batch_size -> fewer runs, faster merge, more RAM per worker.
    - Set max_workers ~= number of physical cores (or cores-1).

    Returns:
        List of absolute run file paths produced.
    """
    ensure_dir(outdir)
    run_paths: List[str] = []

    futures = []
    docid = start_docid
    batch_idx = 0
    batch_lines: List[str] = []

    with ProcessPoolExecutor(max_workers=max_workers) as ex:
        # Stream the TSV, accumulate a batch of raw lines
        for line in read_tsv_stream(input_tsv):
            batch_lines.append(line)
            if len(batch_lines) >= batch_size:
                run_path = os.path.join(outdir, f"run_{batch_idx:06d}.tsv")
                # submit a copy of current batch
                futures.append(ex.submit(_worker_build_run, list(batch_lines), docid, run_path))
                run_paths.append(run_path)
                # advance counters
                docid += len(batch_lines)
                batch_idx += 1
                batch_lines.clear()

        # tail batch
        if batch_lines:
            run_path = os.path.join(outdir, f"run_{batch_idx:06d}.tsv")
            futures.append(ex.submit(_worker_build_run, list(batch_lines), docid, run_path))
            run_paths.append(run_path)
            docid += len(batch_lines)
            batch_idx += 1
            batch_lines.clear()

        # Collect doc lengths from all workers
        # (Avoid shared mutable state; aggregate in main)
        doc_lengths: Dict[int, int] = {}
        total_docs = 0
        total_rows = 0

        for fut in as_completed(futures):
            n_docs, n_rows, doclen_pairs = fut.result()
            total_docs += n_docs
            total_rows += n_rows
            for d, l in doclen_pairs:
                doc_lengths[d] = l

    # Persist doc lengths once (consistent with run docIDs)
    write_doc_lengths(doc_lengths, DOC_LENGTHS_PATH)
    print(f"[BuildRunsMP] Doc lengths saved to {DOC_LENGTHS_PATH}  N={len(doc_lengths)}")
    print(f"[BuildRunsMP] Total docs={total_docs}  total rows={total_rows}  runs={len(run_paths)}")

    return run_paths


def main():
    ap = argparse.ArgumentParser(description="Build sorted TSV runs in parallel (multiprocessing).")
    ap.add_argument("--input", required=True, help="Input TSV, e.g., data/marco_medium.tsv")
    ap.add_argument("--outdir", default="data/runs", help="Directory to write run_*.tsv")
    ap.add_argument("--batch-size", type=int, default=200_000, help="Docs per run (tune for memory)")
    ap.add_argument("--start-docid", type=int, default=0, help="Starting docid")
    ap.add_argument("--workers", type=int, default=None, help="#processes; default: os.cpu_count()")
    args = ap.parse_args()

    build_runs_mp(
        input_tsv=args.input,
        outdir=args.outdir,
        batch_size=args.batch_size,
        start_docid=args.start_docid,
        max_workers=args.workers,
    )


if __name__ == "__main__":
    # IMPORTANT for Windows multiprocessing
    # well GPT says so but idk why
    main()
