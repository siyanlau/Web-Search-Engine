# tests/test_daat_boolean.py
import random
import pytest

from engine.paths import LEXICON_PATH, POSTINGS_PATH
from engine.lexicon import Lexicon
from engine.listio import ListReader
from engine.daat import PostingsCursor, boolean_and_daat, boolean_or_daat
from engine.searcher import Searcher

RANDOM_SEED = 42
SAMPLE_QUERIES = 100
TERMS_PER_QUERY = 2

def build_daat_set(q: str, lex_map, reader, mode="AND"):
    terms = [t for t in q.lower().split() if t in lex_map]
    if not terms:
        return set()
    cursors = [PostingsCursor(reader, t, lex_map[t]) for t in terms]
    if mode == "AND":
        return set(boolean_and_daat(cursors))
    else:
        return set(boolean_or_daat(cursors))

def to_docid_set(obj):
    """Normalize Searcher output to a set of docids."""
    if isinstance(obj, set):
        return obj
    if isinstance(obj, list):
        if not obj:
            return set()
        first = obj[0]
        # BM25 path: list[(docid, score)]
        if isinstance(first, tuple) and len(first) == 2:
            return {d for d, _ in obj}
        # Boolean path (if it ever returns list of ints)
        if isinstance(first, int):
            return set(obj)
    # Fallback
    return set()

def test_daat_matches_existing_boolean():
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)
    s = Searcher(lexicon_path=LEXICON_PATH, postings_path=POSTINGS_PATH, doc_lengths=None)

    terms = list(lex.keys())
    assert terms, "Lexicon is empty; build the index first."

    random.seed(RANDOM_SEED)
    queries = []
    for _ in range(SAMPLE_QUERIES):
        qs = random.sample(terms, TERMS_PER_QUERY)
        queries.append(" ".join(qs))

    for q in queries:
        got_and = build_daat_set(q, lex, reader, mode="AND")
        exp_and = to_docid_set(s.search(q, mode="AND"))
        assert got_and == exp_and, f"AND mismatch for query: {q}"

        got_or = build_daat_set(q, lex, reader, mode="OR")
        exp_or = to_docid_set(s.search(q, mode="OR"))
        assert got_or == exp_or, f"OR mismatch for query: {q}"

    reader.close()
