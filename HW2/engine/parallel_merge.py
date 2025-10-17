# engine/parallel_merge.py
"""
Parallel multi-round merging of intermediate runs.

Strategy
--------
- Input: a list (or glob) of sorted runs. Each run yields (term:str, docid:int, tf:int)
  in strictly (term, docid) order. Runs can be legacy TSV or binary RUN1.
- We perform layered merging:
    round 0: split K runs into groups of size <= fanin, merge each group in parallel
    round 1: repeat on the newly produced runs
    ...
  until the number of runs <= fanin or == 1.
- The default setting only uses 1 layer, because it's sufficient, given we have 89 runs in total to merge.
- Each group-merge produces *another run* (binary RUN1), NOT the final index.
  After the last round, use engine.merger to write the final postings/index.

CLI
---
  python -m engine.parallel_merge data/runs/*.run
  python -m engine.parallel_merge --fanin 8 --workers 8 --tmpdir data/tmp_merge data/runs/*.tsv
Set the number of workers to the amount of physical cores!

To finish indexing and produce a single large postings file (in blocked, binary format):
  python -m engine.merger data/tmp_merge/round_000*/run_*.run
"""

from __future__ import annotations

import argparse
import glob
import math
import multiprocessing as mp
import os
import sys
from heapq import merge as kmerge
from typing import Iterable, Iterator, List, Sequence, Tuple

# Readers/Writers
from engine.merger import open_run_reader          # auto-detect TSV vs RUN1  :contentReference[oaicite:2]{index=2}
from engine.runio import BinaryRunWriter            # always write RUN1        :contentReference[oaicite:3]{index=3}


# --------------------------
# utils
# --------------------------

def _expand_globs(paths: Sequence[str]) -> List[str]:
    out: List[str] = []
    for p in paths:
        if any(ch in p for ch in "*?[]"):
            out.extend(sorted(glob.glob(p)))
        else:
            out.append(p)
    return out


def _chunks(xs: Sequence[str], n: int) -> Iterator[Sequence[str]]:
    for i in range(0, len(xs), n):
        yield xs[i : i + n]


# --------------------------
# single-group worker
# --------------------------

def _merge_group_to_run(run_paths: Sequence[str], out_path: str) -> Tuple[str, int]:
    """
    Merge a small group of runs (size <= fanin) into a single RUN1 file.

    Output is strictly (term, docid) sorted; for identical (term, docid) we sum tfs.
    Returns (out_path, postings_emitted).
    """
    readers = [open_run_reader(p) for p in run_paths]
    stream = kmerge(*(iter(r) for r in readers), key=lambda x: (x[0], x[1]))

    postings = 0
    with BinaryRunWriter(out_path) as w:
        current_term = None
        last_docid = -1
        cur_tf = 0  # for current (term, docid)
        have_doc = False

        def flush_doc():
            nonlocal have_doc, cur_tf
            if have_doc:
                w.add(current_term, last_docid, cur_tf)
                have_doc = False

        for term, docid, tf in stream:
            if current_term is None:
                current_term = term
                last_docid = docid
                cur_tf = tf
                have_doc = True
            elif term != current_term:
                # finish previous term’s tail doc and move on
                flush_doc()
                current_term = term
                last_docid = docid
                cur_tf = tf
                have_doc = True
            else:
                # same term
                if have_doc and docid == last_docid:
                    cur_tf += tf
                else:
                    flush_doc()
                    last_docid = docid
                    cur_tf = tf
                    have_doc = True
            postings += 1

        # finalize tail
        flush_doc()

    # Close readers
    for r in readers:
        close = getattr(r, "close", None)
        if callable(close):
            try:
                close()
            except Exception:
                pass

    return out_path, postings


def _worker(entry):
    # entry = (paths, out_path)
    paths, out_path = entry
    try:
        path, n = _merge_group_to_run(paths, out_path)
        return (path, n, None)
    except Exception as e:
        return (out_path, 0, repr(e))


# --------------------------
# main driver (multi-round)
# --------------------------

def parallel_merge(inputs: Sequence[str], *, fanin: int = 12,
                   workers: int = max(1, os.cpu_count() // 2),
                   tmpdir: str = "data/tmp_merge", verbose: bool = True,
                   rounds: int | None = None) -> List[str]:
    """
    Multi-round layered merge. Returns the list of output run paths from the last round.
    """
    os.makedirs(tmpdir, exist_ok=True)
    cur = list(inputs)

    round_idx = 0
    while len(cur) > 1:
        groups = list(_chunks(cur, fanin))
        if verbose:
            print(f"[pmerge] round {round_idx} | inputs={len(cur)} | groups={len(groups)} | fanin={fanin} | workers={workers}", file=sys.stderr)

        # prepare output paths
        round_dir = os.path.join(tmpdir, f"round_{round_idx:04d}")
        os.makedirs(round_dir, exist_ok=True)

        tasks = []
        for gi, g in enumerate(groups):
            out_path = os.path.join(round_dir, f"run_{gi:06d}.run")  # RUN1 always
            tasks.append((list(g), out_path))

        # run workers
        out_paths: List[str] = []
        if workers <= 1 or len(tasks) == 1:
            for t in tasks:
                path, cnt, err = _worker(t)
                if err:
                    raise RuntimeError(f"group failed: {t[0]} -> {t[1]} | {err}")
                if verbose:
                    print(f"[pmerge]   group ok: {os.path.basename(path)} | postings={cnt:,}", file=sys.stderr)
                out_paths.append(path)
        else:
            with mp.Pool(processes=workers) as pool:
                for path, cnt, err in pool.imap_unordered(_worker, tasks, chunksize=1):
                    if err:
                        raise RuntimeError(f"group failed: -> {path} | {err}")
                    if verbose:
                        print(f"[pmerge]   group ok: {os.path.basename(path)} | postings={cnt:,}", file=sys.stderr)
                    out_paths.append(path)

        # next round inputs
        cur = sorted(out_paths)
        round_idx += 1
        
        # stop if user limited the number of rounds
        if rounds is not None and round_idx >= rounds:
            break

        # stop early if already small enough
        if len(cur) <= fanin:
            break

    if verbose:
        print(f"[pmerge] done | outputs={len(cur)}", file=sys.stderr)
    return cur


# --------------------------
# CLI
# --------------------------

def main():
    ap = argparse.ArgumentParser(description="Parallel layered merging of runs (outputs RUN1).")
    ap.add_argument("runs", nargs="+", help="Input runs (glob or list). TSV or RUN1.")
    ap.add_argument("--fanin", type=int, default=12, help="Group size per merge job.")
    ap.add_argument("--workers", type=int, default=max(1, (os.cpu_count() or 4) // 2), help="Parallel workers.")
    ap.add_argument("--tmpdir", default="data/tmp_merge", help="Directory for intermediate rounds.")
    ap.add_argument("--quiet", action="store_true", help="Less logging.")
    ap.add_argument("--rounds", type=int, default=None, help="Number of rounds to run (default: until <= fanin).")
    args = ap.parse_args()

    inputs = _expand_globs(args.runs)
    if not inputs:
        print("No input runs.", file=sys.stderr)
        sys.exit(2)

    outs = parallel_merge(inputs, fanin=args.fanin, workers=args.workers, tmpdir=args.tmpdir, verbose=not args.quiet, rounds=args.rounds)
    # Print outputs (one per line) so the caller can pipe them into merger.py
    for p in outs:
        print(p)

if __name__ == "__main__":
    main()
