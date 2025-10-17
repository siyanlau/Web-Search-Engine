import bisect
from typing import Optional, List, Iterable, Tuple

from engine.listio import ListReader
from engine.lexicon import Lexicon


class PostingsCursor:
    """
    Cursor over a single term's postings with block-aware stepping.

    State:
      - current block (docids[], freqs[], last_docid)
      - in-block position 'j' (0..len(docids))
      - global exhausted flag
    """

    __slots__ = ("reader", "entry", "term",
                 "block_index", "block_last",
                 "docids", "freqs", "j", "exhausted")

    def __init__(self, reader: ListReader, term: str, entry: dict):
        self.reader = reader
        self.entry = entry
        self.term = term

        self.block_index = -1
        self.block_last = -1
        self.docids: List[int] = []
        self.freqs: List[int] = []
        self.j = 0
        self.exhausted = (entry["df"] == 0)

        if not self.exhausted:
            # Load first block
            hit = self.reader.seek_block_ge(entry, -1)  # get first block
            if hit is None:
                # empty postings (defensive)
                self.exhausted = True
            else:
                bidx, last_docid, d, f = hit
                self.block_index = bidx
                self.block_last = last_docid
                self.docids, self.freqs = d, f
                self.j = 0
                if not self.docids:
                    self.exhausted = True

    def _load_block(self, bidx: int) -> bool:
        """Load block by absolute index (using entry['blocks'] for offset)."""
        blocks = self.entry.get("blocks")
        if not blocks:
            # Fallback: linear iteration to bidx
            # We start from entry['offset'] and scan; this is slower but correct.
            # For simplicity, use iter_blocks and advance until the desired index.
            count = 0
            for last_docid, d, f in self.reader.iter_blocks(self.entry):
                if count == bidx:
                    self.block_index = bidx
                    self.block_last = last_docid
                    self.docids, self.freqs = d, f
                    self.j = 0
                    return True
                count += 1
            return False
        # Fast path: use seek on the exact block
        if bidx < 0 or bidx >= len(blocks):
            return False
        # Reuse seek_block_ge logic to load exact block
        target = blocks[bidx]["last_docid"]
        hit = self.reader.seek_block_ge(self.entry, target)
        if hit is None:
            return False
        got_bidx, last_docid, d, f = hit
        # seek_block_ge returns the first block whose last_docid >= target;
        # since 'target' is exactly this block's last_docid, got_bidx == bidx.
        self.block_index = got_bidx
        self.block_last = last_docid
        self.docids, self.freqs = d, f
        self.j = 0
        return True

    def docid(self) -> Optional[int]:
        if self.exhausted or self.j >= len(self.docids):
            return None
        return self.docids[self.j]

    def advance(self) -> Optional[int]:
        """Move to next posting in the same term."""
        if self.exhausted:
            return None
        self.j += 1
        if self.j < len(self.docids):
            return self.docids[self.j]
        # Need next block
        next_b = self.block_index + 1
        if not self._load_block(next_b):
            self.exhausted = True
            return None
        return self.docids[self.j]

    def next_ge(self, target_docid: int) -> Optional[int]:
        """
        Advance to the first posting with docid >= target_docid.
        Returns the current docid or None if exhausted.
        """
        if self.exhausted:
            return None

        # If target within current block range, just lower_bound inside block.
        if target_docid <= self.block_last:
            j = bisect.bisect_left(self.docids, target_docid, lo=self.j)
            if j < len(self.docids):
                self.j = j
                return self.docids[self.j]
            # else we fell off the end -> load next block
            next_b = self.block_index + 1
            if not self._load_block(next_b):
                self.exhausted = True
                return None
            # After load, fall through to block-level seek (below)

        # target beyond current block: use block-level seek
        hit = self.reader.seek_block_ge(self.entry, target_docid)
        if hit is None:
            self.exhausted = True
            return None
        bidx, last_docid, d, f = hit
        self.block_index = bidx
        self.block_last = last_docid
        self.docids, self.freqs = d, f
        # lower_bound inside this block
        self.j = bisect.bisect_left(self.docids, target_docid)
        if self.j >= len(self.docids):
            # no doc in this block >= target; try next block
            if not self._load_block(bidx + 1):
                self.exhausted = True
                return None
            self.j = 0
        return self.docids[self.j]
