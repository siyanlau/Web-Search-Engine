# engine/daat_ranker.py
"""
DAAT + BM25 Top-K ranking (no pruning).

This module is additive: it does not modify any existing interfaces.
It consumes:
  - a lexicon map: term -> entry (must include at least 'df'; 'blocks' speeds up seeking)
  - a ListReader opened on the postings file
  - a doc_lengths dict: docid -> document length

It produces the top-K results for a query by streaming documents in
increasing docid order (k-way merge over per-term cursors), accumulating
BM25 scores per document, and maintaining a size-K min-heap.

Matching model:
  - mode="OR" (default): any document that matches >=1 query term is eligible.
  - mode="AND": only documents that match *all* query terms are eligible.

Pruning:
  - None yet. This is the correctness-first version. Block-max / WAND-style
    upper bounds can be layered on top without changing the public API.
"""

from __future__ import annotations

import heapq
import math
from collections import defaultdict
from typing import Dict, List, Tuple

from engine.daat import PostingsCursor
from engine.listio import ListReader


def _bm25_idf(N: int, df: int) -> float:
    """Standard BM25 IDF (with +1 in log to avoid negative edge cases)."""
    return math.log((N - df + 0.5) / (df + 0.5) + 1.0)


def _bm25_term(tf: int, df: int, dl: int, N: int, avgdl: float, k1: float, b: float) -> float:
    """BM25 single-term contribution for a (doc, term) pair."""
    idf = _bm25_idf(N, df)
    denom = tf + k1 * (1.0 - b + b * (dl / avgdl))
    return idf * (tf * (k1 + 1.0)) / denom


def ranked_daat(
    query: str,
    lex_map: Dict[str, dict],
    reader: ListReader,
    doc_lengths: Dict[int, int],
    topk: int = 10,
    k1: float = 1.2,
    b: float = 0.75,
    mode: str = "OR",  # "OR" (default) or "AND"
) -> List[Tuple[int, float]]:
    """
    Rank documents for `query` using DAAT + BM25 (no pruning).
    Returns: list[(docid, score)] sorted by score desc.

    Notes
    -----
    - Tokenization here mirrors the simple query.split() you use elsewhere.
      If you later plug in the Parser's tokenizer, swap it in at the call-site.
    - For mode="AND", only documents present in *all* term streams are scored.
    """
    # Tokenize query and keep only terms known to the lexicon
    terms: List[str] = [t for t in query.lower().split() if t in lex_map]
    if not terms:
        return []

    # Corpus stats
    N = len(doc_lengths)
    if N == 0:
        return []
    avgdl = sum(doc_lengths.values()) / N

    # Sort terms by ascending df (classic heuristic to reduce cursor work)
    terms.sort(key=lambda t: lex_map[t]["df"])

    # Build a cursor per term
    cursors: List[PostingsCursor] = [PostingsCursor(reader, t, lex_map[t]) for t in terms]

    # Initialize k-way merge over current docids
    heap: List[Tuple[int, int]] = []  # (docid, cursor_index)
    for i, cur in enumerate(cursors):
        d = cur.docid()
        if d is not None:
            heap.append((d, i))
    heapq.heapify(heap)
    if not heap:
        return []

    # Accumulators
    scores: defaultdict[int, float] = defaultdict(float)
    top: List[Tuple[float, int]] = []  # min-heap of (score, docid)
    dfs = {t: lex_map[t]["df"] for t in terms}

    while heap:
        # Pop the smallest docid and gather all cursors tied on this docid
        d, i = heapq.heappop(heap)
        tied = [i]
        while heap and heap[0][0] == d:
            _, j = heapq.heappop(heap)
            tied.append(j)

        # AND gating: require the doc to appear in all streams
        if mode.upper() == "AND" and len(tied) < len(cursors):
            # Do not score this doc; just advance tied cursors and continue
            for idx in tied:
                nxt = cursors[idx].advance()
                if nxt is not None:
                    heapq.heappush(heap, (nxt, idx))
            continue

        # Accumulate BM25 contributions from each matched term
        for idx in tied:
            cur = cursors[idx]
            tf = cur.freqs[cur.j]  # safe: (docid, tf) at current position
            t = cur.term
            dl = doc_lengths.get(d, 0)
            if dl > 0:
                scores[d] += _bm25_term(tf, dfs[t], dl, N, avgdl, k1, b)

        # Maintain top-K (min-heap by score)
        sc = scores[d]
        if len(top) < topk:
            heapq.heappush(top, (sc, d))
        else:
            if sc > top[0][0]:
                heapq.heapreplace(top, (sc, d))

        # Advance all tied cursors and push their next docids
        for idx in tied:
            nxt = cursors[idx].advance()
            if nxt is not None:
                heapq.heappush(heap, (nxt, idx))

    # Emit top-K sorted by score desc
    top.sort(key=lambda x: x[0], reverse=True)
    return [(docid, score) for (score, docid) in top]
