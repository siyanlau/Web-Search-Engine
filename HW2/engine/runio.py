# engine/runio.py
"""
Utilities for writing and reading intermediate *sorted* posting runs
in a simple TSV format:  term<TAB>docid<TAB>tf

These runs are used by the k-way merger to produce the final blocked index.
"""

import os

class RunWriter:
    """
    Writes a single *sorted* run file from an in-memory posting map.

    Input format:
        postings: dict[str, dict[int, int]]  # term -> {docid: tf}

    The writer guarantees the output is globally sorted by (term, docid).
    """
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
