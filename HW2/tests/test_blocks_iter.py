# tests/test_blocks_iter_consistency.py
import random
import pytest

from engine.paths import LEXICON_PATH, POSTINGS_PATH
from engine.lexicon import Lexicon
from engine.listio import ListReader

RANDOM_SEED = 2025
SAMPLE_TERMS = 200 

def test_iter_blocks_matches_read_postings():
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)

    terms = list(lex.keys())
    assert terms, "Lexicon is empty; build the index first."
    random.seed(RANDOM_SEED)
    sample = random.sample(terms, min(SAMPLE_TERMS, len(terms)))

    for t in sample:
        entry = lex[t]

        # read_postings as standard
        docids_full, freqs_full = reader.read_postings(entry)
        assert len(docids_full) == entry["df"]
        assert len(docids_full) == len(freqs_full)

        
        cat_docids, cat_freqs = [], []
        prev_last = -1
        for last_docid, d, f in reader.iter_blocks(entry):
            for i in range(1, len(d)):
                assert d[i] > d[i-1], f"DocIDs must be strictly increasing inside a block for term '{t}'"
            assert d[-1] == last_docid, f"Block header last_docid mismatch for term '{t}'"
            if prev_last != -1:
                assert d[0] > prev_last, f"First docid of block must be > previous block's last for term '{t}'"
            prev_last = last_docid

            cat_docids.extend(d)
            cat_freqs.extend(f)

        assert cat_docids == docids_full, f"Concatenated docids mismatch for term '{t}'"
        assert cat_freqs == freqs_full, f"Concatenated freqs mismatch for term '{t}'"

    reader.close()


def test_iter_blocks_linear_fallback_without_blocks_dir():
    """Remove 'blocks' from entry to force linear scan path and check equivalence."""
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)

    terms = list(lex.keys())
    random.seed(RANDOM_SEED + 1)
    sample = random.sample(terms, min(50, len(terms)))

    for t in sample:
        entry = dict(lex[t])         # shallow copy
        entry.pop("blocks", None)    # force fallback

        from_entry = lex[t]
        docids_ref, freqs_ref = reader.read_postings(from_entry)

        docids_got, freqs_got = [], []
        prev_last = -1
        total_seen = 0
        for last_docid, d, f in reader.iter_blocks(entry):
            # fallback should also work
            for i in range(1, len(d)):
                assert d[i] > d[i-1]
            assert d[-1] == last_docid
            if prev_last != -1:
                assert d[0] > prev_last
            prev_last = last_docid

            docids_got.extend(d)
            freqs_got.extend(f)
            total_seen += len(d)

        assert total_seen == entry["df"]
        assert docids_got == docids_ref
        assert freqs_got == freqs_ref

    reader.close()
