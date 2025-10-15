# engine/runio.py
"""
Utilities for writing and reading intermediate *sorted* posting runs
in a simple TSV format:  term<TAB>docid<TAB>tf

These runs are used by the k-way merger to produce the final blocked index.
"""

import os
import sys


class RunWriter:
    """
    Writes a single *sorted* run file from an in-memory posting map.

    Input format:
        postings: dict[str, dict[int, int]]  # term -> {docid: tf}

    The writer guarantees the output is globally sorted by (term, docid).
    """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.close()
        except Exception:
            pass
    
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self._f = open(path, "w", encoding="utf-8")

    def write_from_index(self, postings: dict[str, dict[int, int]]):
        # Sort by term, then by docid
        for term in sorted(postings.keys()): # two step sort, reduce complexity
            plist = postings[term]
            for docid in sorted(plist.keys()):
                tf = plist[docid]
                self._f.write(f"{term}\t{docid}\t{tf}\n")

    def close(self):
        self._f.close()


class RunReader:
    """
    Sequentially reads a run file produced by RunWriter.

    Yields tuples: (term: str, docid: int, tf: int)
    """
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        try:
            self.close()
        except Exception:
            pass
    
    def __init__(self, path: str):
        self.path = path
        self._f = open(path, "r", encoding="utf-8")
        self._next = None

    def __iter__(self):
        return self

    def __next__(self):
        line = self._f.readline()
        if not line:
            self._f.close()
            raise StopIteration
        term, docid, tf = line.rstrip("\n").split("\t")
        return term, int(docid), int(tf)



# -------------- binary runs----------------------- (trying to cut down tsv processing time)
import io
import os
import struct
from typing import Iterator, Tuple, Optional

MAGIC = b"RUN1"
_U32 = struct.Struct("<I")

def _read_u32(f: io.BufferedReader) -> int:
    b = f.read(4)
    if len(b) != 4:
        raise EOFError
    return _U32.unpack(b)[0]

def _write_u32(f: io.BufferedWriter, x: int) -> None:
    f.write(_U32.pack(x))

class BinaryRunWriter:
    """
    Write a run in grouped-binary format:
    [MAGIC] [ for each term: len_term, term_utf8, n, docid[n], freq[n] ].
    Call add(term, docid, freq) in sorted (term, docid) order.
    """
    __slots__ = ("path", "file", "_cur_term", "_doc_buf", "_freq_buf")

    def __init__(self, path: str):
        self.path = path
        self.file = open(path, "wb", buffering=1024 * 1024)
        self.file.write(MAGIC)
        self._cur_term: Optional[str] = None
        self._doc_buf = bytearray()
        self._freq_buf = bytearray()

    def _flush_group(self):
        if self._cur_term is None:
            return
        term_b = self._cur_term.encode("utf-8")
        n = len(self._doc_buf) // 4
        # [len_term][term][n][docids][freqs]
        _write_u32(self.file, len(term_b))
        self.file.write(term_b)
        _write_u32(self.file, n)
        self.file.write(self._doc_buf)
        self.file.write(self._freq_buf)
        # reset buffers
        self._doc_buf.clear()
        self._freq_buf.clear()

    def add(self, term: str, docid: int, freq: int):
        # Term changed => flush previous group
        if self._cur_term != term:
            self._flush_group()
            self._cur_term = term
        self._doc_buf += _U32.pack(docid)
        self._freq_buf += _U32.pack(freq)

    def close(self):
        self._flush_group()
        self.file.flush()
        self.file.close()

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb):
        try: self.close()
        finally: return False


class BinaryRunReader:
    """
    Iterate (term, docid, freq) from a binary run file written by BinaryRunWriter.
    The iterator streams postings group-by-group with minimal allocations.
    """
    __slots__ = ("path", "file", "_term", "_doc_view", "_freq_view", "_n", "_i")

    def __init__(self, path: str):
        self.path = path
        self.file = open(path, "rb", buffering=1024 * 1024)
        magic = self.file.read(4)
        if magic != MAGIC:
            raise ValueError(f"{path}: bad magic {magic!r}, expected {MAGIC!r}")
        # Current group state
        self._term: Optional[str] = None
        self._doc_view = memoryview(b"")
        self._freq_view = memoryview(b"")
        self._n = 0
        self._i = 0

    def _load_next_group(self) -> bool:
        # Returns False on EOF
        b = self.file.read(4)
        if not b:
            return False  # EOF cleanly
        if len(b) != 4:
            raise EOFError("Truncated group header (len_term)")
        len_term = _U32.unpack(b)[0]
        term_b = self.file.read(len_term)
        if len(term_b) != len_term:
            raise EOFError("Truncated term bytes")
        self._term = term_b.decode("utf-8")

        n = _read_u32(self.file)
        # We don't copy; we keep memoryviews over the file's buffers
        doc_bytes = self.file.read(4 * n)
        if len(doc_bytes) != 4 * n:
            raise EOFError("Truncated docids")
        freq_bytes = self.file.read(4 * n)
        if len(freq_bytes) != 4 * n:
            raise EOFError("Truncated freqs")

        self._doc_view = memoryview(doc_bytes)
        self._freq_view = memoryview(freq_bytes)
        self._n = n
        self._i = 0
        return True

    def __iter__(self) -> Iterator[Tuple[str, int, int]]:
        return self

    def __next__(self) -> Tuple[str, int, int]:
        # Exhausted current group? load the next one.
        while self._i >= self._n:
            if not self._load_next_group():
                raise StopIteration
        # Fast unpack using struct.unpack_from on memoryview
        off = self._i * 4
        docid = _U32.unpack_from(self._doc_view, off)[0]
        freq  = _U32.unpack_from(self._freq_view, off)[0]
        term = self._term  # local ref
        self._i += 1
        return term, docid, freq

    def close(self):
        try:
            self.file.close()
        finally:
            self._doc_view = memoryview(b"")
            self._freq_view = memoryview(b"")

    def __enter__(self): return self
    def __exit__(self, exc_type, exc, tb):
        try: self.close()
        finally: return False