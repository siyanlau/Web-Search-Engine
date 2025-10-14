"""
engine/lexicon.py

Lexicon maps each term to its on-disk postings metadata.

Each entry is a small dict like:
    {
        "offset": int,        # byte offset of the first block in index.postings
        "df": int,             # total document frequency (docs containing this term)
        "nblocks": int,        # number of blocks for this term
        "blocks": [            # optional: per-block directory (for fast seek)
            {
                "offset": int,       # byte offset of this block in postings file
                "last_docid": int,   # last docid within this block
                "doc_bytes": int,    # length of encoded docid segment
                "freq_bytes": int    # length of encoded frequency segment
            },
            ...
        ]
    }

This structure enables:
  - O(log B) random access via block directory
  - efficient sequential streaming via ListReader.iter_blocks()

Stored as a pickle file for simplicity.
"""

import pickle

class Lexicon:
    """
    Persistent mapping from term -> on-disk posting metadata.

    Typical usage:
        lex = Lexicon()
        lex.add("hello", {"offset": 1234, "df": 10, "nblocks": 2, "blocks": [...]})
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
