# engine/merger.py
"""
K-way merger for intermediate sorted runs.

Reads multiple TSV runs (term, docid, tf), merges them by (term, docid),
accumulates tf for the same (term, docid), and writes the final blocked
inverted index via ListWriter + Lexicon.

Compression-agnostic: swapping ListWriter's internal codec later requires
no changes here.
"""

import heapq
from collections import defaultdict
from typing import List, Tuple

from engine.runio import RunReader
from engine.listio import ListWriter
from engine.lexicon import Lexicon

def merge_runs_to_index(run_paths, postings_path, lexicon_path, block_size=128):
    """
    K-way merge over sorted TSV runs:
      - inputs: list of run files (term, docid, tf) sorted by (term, docid)
      - output: postings_path (blocked binary) + lexicon_path (pickle)

    For each term:
      - aggregate tf if the same (term, docid) appears across runs
      - call ListWriter.add_term(term, {docid: tf})
      - lexicon.add(term, returned_entry)
    """
    # open all runs
    readers = [RunReader(p) for p in run_paths]

    # init heap with first row from each run
    heap = []  # (term, docid, run_idx, tf)
    for i, r in enumerate(readers):
        try:
            t, d, tf = next(r)
            heap.append((t, d, i, tf))
        except StopIteration:
            pass
    heapq.heapify(heap)

    writer = ListWriter(postings_path, block_size=block_size)
    lex = Lexicon()

    cur_term = None
    cur_postings = defaultdict(int)  # docid -> tf

    def flush_term():
        nonlocal cur_term, cur_postings
        if cur_term is None:
            return
        entry = writer.add_term(cur_term, cur_postings)   # writes blocks
        lex.add(cur_term, entry)                          # records offsets/blocks
        cur_postings.clear()

    while heap:
        term, docid, i, tf = heapq.heappop(heap)

        # new term boundary -> flush previous term
        if cur_term is None:
            cur_term = term
        elif term != cur_term:
            flush_term()
            cur_term = term

        # aggregate tf for this (term, docid)
        cur_postings[docid] += tf

        # advance that run
        try:
            t2, d2, tf2 = next(readers[i])
            heapq.heappush(heap, (t2, d2, i, tf2))
        except StopIteration:
            pass

    # flush last term
    flush_term()

    # close & persist
    writer.close()
    lex.save(lexicon_path)

    # close runs
    for r in readers:
        try:
            r._f.close()
        except Exception:
            pass

class Merger:
    """
    Merge multiple sorted runs into final blocked postings + lexicon.
    """

    def __init__(self, postings_path: str, lexicon_path: str, block_size: int = 128):
        self.postings_path = postings_path
        self.lexicon_path = lexicon_path
        self.block_size = block_size

    def merge(self, run_paths: List[str]):
        """
        Args:
            run_paths: list of file paths to TSV runs (each sorted by term,docid).

        Produces:
            - postings_path (binary blocked postings, via ListWriter)
            - lexicon_path  (pickle term->entry)
        """
        readers = [RunReader(p) for p in run_paths]
        # Min-heap of (term, docid, tf, run_idx)
        heap: List[Tuple[str, int, int, int]] = []

        # Prime heap: pull first item from each run if any
        for i, r in enumerate(readers):
            try:
                t, d, tf = next(r)
                heap.append((t, d, tf, i))
            except StopIteration:
                pass
        heapq.heapify(heap)

        writer = ListWriter(self.postings_path, block_size=self.block_size)
        lex = Lexicon()

        # Current accumulation for a term
        current_term = None
        accum: defaultdict[int, int] = defaultdict(int)  # docid -> tf

        while heap:
            term, docid, tf, src = heapq.heappop(heap)

            # If term changes, flush previous term
            if current_term is None:
                current_term = term
            if term != current_term:
                if accum:
                    entry = writer.add_term(current_term, accum)
                    lex.add(current_term, entry)
                    accum.clear()
                current_term = term

            # Accumulate tf for same (term, docid)
            accum[docid] += tf

            # Advance the source run
            try:
                t2, d2, tf2 = next(readers[src])
                heapq.heappush(heap, (t2, d2, tf2, src))
            except StopIteration:
                pass

        # Flush the last term
        if current_term is not None and accum:
            entry = writer.add_term(current_term, accum)
            lex.add(current_term, entry)

        writer.close()
        lex.save(self.lexicon_path)
        print(f"[Merger] Wrote postings -> {self.postings_path}")
        print(f"[Merger] Wrote lexicon  -> {self.lexicon_path}")


if __name__ == "__main__":
    # Example CLI (run from repo root):
    #   python -m engine.merger data/runs/run1.tsv data/runs/run2.tsv
    import sys
    if len(sys.argv) < 2:
        print("Usage: python -m engine.merger <run1.tsv> <run2.tsv> ...")
        sys.exit(1)

    from engine.paths import POSTINGS_PATH, LEXICON_PATH
    merger = Merger(POSTINGS_PATH, LEXICON_PATH, block_size=128)
    merger.merge(sys.argv[1:])
