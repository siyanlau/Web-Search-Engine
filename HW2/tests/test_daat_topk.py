# tests/test_daat_topk.py
import random
import pytest

from engine.paths import LEXICON_PATH, POSTINGS_PATH, DOC_LENGTHS_PATH
from engine.lexicon import Lexicon
from engine.listio import ListReader
from engine.utils import load_doc_lengths
from engine.searcher import Searcher
from engine.daat_ranker import ranked_daat

RANDOM_SEED = 99
QUERIES = [
    "overturned carriage",
    "communication policy",
    "machine learning",
    "u.s policy",
    "3.14 math",
]

def test_ranked_daat_matches_ranker_topk():
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)
    doc_lengths = load_doc_lengths(DOC_LENGTHS_PATH)

    s = Searcher()  # your existing BM25 path
    random.seed(RANDOM_SEED)

    for q in QUERIES:
        # baseline
        base = s.search(q, topk=10)  # list[(docid, score)]
        base_ids = [d for d, _ in base]

        # daat
        got = ranked_daat(q, lex, reader, doc_lengths, topk=10, mode="AND")
        got_ids = [d for d, _ in got]

        # Compare as ordered lists; if tiny floating noise causes tie flips,
        # at least sets must match.
        assert set(got_ids) == set(base_ids), f"Top-10 doc set mismatch for query: {q}"
        # Optional stronger check (can relax if a rare tie flips order)
        if got_ids and base_ids:
            assert got_ids[:5] == base_ids[:5], f"Top-5 order mismatch for query: {q}"

    reader.close()
