# engine/merger.py
"""
K-way merger for intermediate sorted runs.

This merges multiple *sorted* runs (each yields (term:str, docid:int, tf:int)
in strictly (term, docid) order) into the final blocked inverted index.

Features
- Supports BOTH legacy TSV runs and the new binary RUN1 runs (auto-detected).
- Writes the final postings via ListWriter and term metadata via Lexicon.
- No dependency on the on-disk postings codec: swap ListWriter(codec=...)
  without touching this module.

Complexity
- Time:  O(TotalPostings * log K) where K is the number of runs.
- Space: O(#unique_docids_in_current_term) for the accumulation dict.
"""

from __future__ import annotations

import heapq
import io
import os
import sys
from collections import defaultdict
from typing import Iterable, List, Tuple

from engine.runio import RunReader, BinaryRunReader  # TSV + RUN1
from engine.listio import ListWriter
from engine.lexicon import Lexicon


# ----------------------------
# Run reader auto-detection
# ----------------------------

def open_run_reader(path: str):
    """
    Factory that opens the proper reader based on file magic.
    - Binary RUN1: first 4 bytes == b"RUN1"  -> BinaryRunReader
    - Otherwise: fall back to TSV RunReader
    """
    try:
        with open(path, "rb") as f:
            hdr = f.read(4)
        if hdr == b"RUN1":
            return BinaryRunReader(path)
    except Exception:
        # Any issue -> treat as TSV
        pass
    return RunReader(path)


# ----------------------------
# Core merge routine
# ----------------------------

def merge_runs_to_index(
    run_paths: Iterable[str],
    postings_path: str,
    lexicon_path: str,
    *,
    block_size: int = 128,
    codec: str = "raw",
    progress_every: int = 1_000_000,
) -> None:
    """
    K-way merge over sorted runs:

    Args:
        run_paths: iterable of run file paths; each run is sorted by (term, docid).
        postings_path: final postings file (binary, written by ListWriter).
        lexicon_path: final lexicon file (pickle, written by Lexicon.save()).
        block_size: ListWriter block size (number of (docid, tf) pairs per block target).
        codec: codec to use inside ListWriter ("raw", "varbyte", ...).
        progress_every: print a progress line after consuming this many postings.

    Behavior:
        For each term, we aggregate tf across runs for identical (term, docid),
        then hand the whole postings dict to ListWriter.add_term().
    """
    # Open all runs with auto-detection
    readers = [open_run_reader(p) for p in run_paths]

    # Min-heap of (term, docid, tf, src_idx)
    heap: List[Tuple[str, int, int, int]] = []

    # Prime the heap with the first record from each run
    for i, r in enumerate(readers):
        try:
            t, d, tf = next(r)
            heap.append((t, d, tf, i))
        except StopIteration:
            pass
    heapq.heapify(heap)

    writer = ListWriter(postings_path, block_size=block_size, codec=codec)
    lex = Lexicon()

    current_term: str | None = None
    accum: defaultdict[int, int] = defaultdict(int)  # docid -> tf

    consumed = 0  # number of postings consumed from input runs

    def flush_current_term():
        nonlocal current_term, accum
        if current_term is None or not accum:
            return
        # ListWriter.add_term takes {docid: tf} and returns an entry
        entry = writer.add_term(current_term, accum)
        lex.add(current_term, entry)
        accum.clear()

    while heap:
        term, docid, tf, src = heapq.heappop(heap)

        # Term boundary -> flush previous postings
        if current_term is None:
            current_term = term
        elif term != current_term:
            flush_current_term()
            current_term = term

        # Aggregate tf for this (term, docid)
        accum[docid] += tf
        consumed += 1
        if progress_every and (consumed % progress_every == 0):
            print(f"[merger] consumed={consumed:,}  heap={len(heap)}  term='{term[:24]}'", file=sys.stderr)

        # Advance the source run
        try:
            t2, d2, tf2 = next(readers[src])
            heapq.heappush(heap, (t2, d2, tf2, src))
        except StopIteration:
            pass

    # Flush the last term
    flush_current_term()

    # Persist outputs
    writer.close()
    lex.save(lexicon_path)

    # Close runs (be permissive; readers may or may not expose .close())
    for r in readers:
        close = getattr(r, "close", None)
        try:
            if callable(close):
                close()
        except Exception:
            pass

    print(f"[merger] DONE  postings -> {postings_path}")
    print(f"[merger] DONE  lexicon  -> {lexicon_path}")


# ----------------------------
# OOP wrapper
# ----------------------------

class Merger:
    """
    Thin OO wrapper around merge_runs_to_index().
    """

    def __init__(self, postings_path: str, lexicon_path: str, *, block_size: int = 128, codec: str = "raw"):
        self.postings_path = postings_path
        self.lexicon_path = lexicon_path
        self.block_size = block_size
        self.codec = codec

    def merge(self, run_paths: List[str]) -> None:
        merge_runs_to_index(
            run_paths,
            postings_path=self.postings_path,
            lexicon_path=self.lexicon_path,
            block_size=self.block_size,
            codec=self.codec,
        )


# ----------------------------
# CLI
# ----------------------------

def _expand_globs(paths: List[str]) -> List[str]:
    # Windows shell may not expand globs; do it here.
    out: List[str] = []
    for p in paths:
        if any(ch in p for ch in "*?[]"):
            import glob
            out.extend(sorted(glob.glob(p)))
        else:
            out.append(p)
    return out


if __name__ == "__main__":
    # Example:
    #   python -m engine.merger data/runs/*.run              # binary runs
    #   python -m engine.merger data/runs/*.tsv              # legacy TSV runs
    #   python -m engine.merger --codec varbyte --block 256 data/runs/*.run
    import argparse
    from engine.paths import POSTINGS_PATH, LEXICON_PATH

    ap = argparse.ArgumentParser(description="K-way merge sorted runs into final index.")
    ap.add_argument("runs", nargs="+", help="Input runs (glob or list). Each must be sorted by (term, docid).")
    ap.add_argument("--postings", default=POSTINGS_PATH, help="Output postings path (binary).")
    ap.add_argument("--lexicon", default=LEXICON_PATH, help="Output lexicon (pickle).")
    ap.add_argument("--block", dest="block_size", type=int, default=128, help="ListWriter block size.")
    ap.add_argument("--codec", default="raw", help="ListWriter codec: raw|varbyte (etc.).")
    ap.add_argument("--progress-every", type=int, default=1_000_000, help="Stderr progress interval in #postings (0=off).")
    args = ap.parse_args()

    run_paths = _expand_globs(args.runs)
    if not run_paths:
        print("No input runs after glob expansion.", file=sys.stderr)
        sys.exit(2)

    merger = Merger(args.postings, args.lexicon, block_size=args.block_size, codec=args.codec)
    merger.merge(run_paths)
