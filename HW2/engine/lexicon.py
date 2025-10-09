"""
engine/lexicon.py

Lexicon maps terms to their on-disk posting metadata.

Each entry is a small dict like:
    {
        "offset": int,     # byte offset of the first block in index.postings
        "df": int,         # document frequency (number of docs containing term)
        "nblocks": int     # number of blocks for this term
    }

This structure allows the searcher to quickly locate the postings
of any term in the binary postings file via ListReader.
"""

import pickle

class Lexicon:
    """
    Persistent mapping from term -> on-disk posting metadata.
    Stored as a pickle file for simplicity.

    Typical usage:
        lex = Lexicon()
        lex.add("hello", {"offset": 1234, "df": 10, "nblocks": 2})
        lex.save("data/index.lexicon")

        # Later:
        lex2 = Lexicon.load("data/index.lexicon")
        entry = lex2.map["hello"]
    """
    def __init__(self):
        self.map = {}

    def add(self, term, entry):
        self.map[term] = entry

    def save(self, path):
        with open(path, "wb") as f:
            pickle.dump(self.map, f)
        print(f"Lexicon saved: {len(self.map)} terms to {path}")

    @classmethod
    def load(cls, path):
        with open(path, "rb") as f:
            data = pickle.load(f)
        lex = cls()
        lex.map = data
        print(f"Lexicon loaded: {len(data)} terms from {path}")
        return lex
