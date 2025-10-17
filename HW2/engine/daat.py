# engine/daat.py
"""
Minimal DAAT (Document-At-A-Time) boolean traversal using block-level APIs.

This module introduces:
  - PostingsCursor: a per-term iterator with next_ge() and advance() primitives
  - boolean_and_daat / boolean_or_daat: DAAT set operations on multiple cursors

It relies only on:
  - Lexicon entry with 'offset/df/nblocks' (and optionally 'blocks' for fast seek)
  - ListReader.iter_blocks() / seek_block_ge()

No existing interfaces are modified; this module is additive.
"""

from typing import Optional, List, Iterable, Tuple

from engine.postings_cursor import PostingsCursor


def boolean_and_daat(cursors: List[PostingsCursor]) -> Iterable[int]:
    """
    DAAT intersection (AND) over multiple postings streams.
    Strategy:
      - Always align all streams to the current candidate docid.
      - Take the maximum of current docids and call next_ge on the others.
      - Stop when any cursor is exhausted.
    """
    if not cursors:
        return []
    # Sort by df (shortest first) would be better, but we don't have df here
    # callers can pre-order by lexicon df if needed.

    # Initialize to the first valid docids
    heads = [cur.docid() for cur in cursors]
    if any(h is None for h in heads):
        return []

    while True:
        target = max(h for h in heads if h is not None)
        advanced_all = True
        for i, cur in enumerate(cursors):
            if heads[i] is None:
                return  # some cursor exhausted
            if heads[i] < target:
                nxt = cur.next_ge(target)
                if nxt is None:
                    return
                heads[i] = nxt
                advanced_all = False
        if advanced_all:
            # All heads equal to target -> intersection hit
            yield target
            # Move all by one
            for i, cur in enumerate(cursors):
                nxt = cur.advance()
                if nxt is None:
                    return
                heads[i] = nxt


def boolean_or_daat(cursors: List[PostingsCursor]) -> Iterable[int]:
    """
    DAAT union (OR) over multiple postings streams.
    Strategy:
      - Multiway merge by current docid.
      - Emit the smallest docid, advance any cursor(s) that are at that docid.
    """
    import heapq
    heap: List[Tuple[int, int]] = []  # (docid, cursor_index)
    for i, cur in enumerate(cursors):
        d = cur.docid()
        if d is not None:
            heap.append((d, i))
    heapq.heapify(heap)

    while heap:
        d, i = heapq.heappop(heap)
        # Emit once for this docid
        yield d
        # Advance any other cursors tied at d
        # First, advance cursor i
        nxt = cursors[i].advance()
        if nxt is not None:
            heapq.heappush(heap, (nxt, i))
        # Drain ties
        while heap and heap[0][0] == d:
            _, j = heapq.heappop(heap)
            nxt = cursors[j].advance()
            if nxt is not None:
                heapq.heappush(heap, (nxt, j))
