# tests/test_seek_block_ge.py
import random
import bisect
import pytest

from engine.paths import LEXICON_PATH, POSTINGS_PATH
from engine.lexicon import Lexicon
from engine.listio import ListReader

RANDOM_SEED = 2025
SAMPLE_TERMS = 120
TARGETS_PER_TERM = 4

def _lower_bound(arr, x):
    # same semantics as bisect_left
    return bisect.bisect_left(arr, x)

def test_seek_block_ge_general_cases():
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)

    terms = list(lex.keys())
    assert terms, "Lexicon is empty; build the index first."
    random.seed(RANDOM_SEED)

    sample = random.sample(terms, min(SAMPLE_TERMS, len(terms)))

    for t in sample:
        entry = lex[t]
        docids_full, freqs_full = reader.read_postings(entry)
        if not docids_full:
            continue

        first = docids_full[0]
        last  = docids_full[-1]
        mids  = [docids_full[len(docids_full)//2]]

        # generate multiple targets
        candidates = {
            first - 1,
            first,
            mids[0],
            last,
            last + 1
        }
        for _ in range(TARGETS_PER_TERM):
            if random.random() < 0.5:
                candidates.add(random.choice(docids_full))
            else:
                candidates.add(random.randint(first, last))

        for target in candidates:
            hit = reader.seek_block_ge(entry, target)
            if target > last:
                # should not find block
                assert hit is None, f"Expected None for term '{t}' target {target} > last {last}"
                continue

            assert hit is not None, f"Expected a block for term '{t}' target {target}"
            bidx, last_docid, d, f = hit
            assert last_docid == d[-1], "Block header last_docid mismatch"
            assert last_docid >= target, "seek_block_ge must return block with last_docid >= target"

            j = _lower_bound(d, target)
            assert 0 <= j <= len(d)
            if j < len(d):
                assert d[j] >= target
            if j > 0:
                assert d[j-1] < target

    reader.close()


def test_seek_block_ge_linear_fallback_without_blocks_dir():
    """Force linear scan by removing 'blocks' and verify semantics unchanged."""
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)

    terms = list(lex.keys())
    random.seed(RANDOM_SEED + 9)
    sample = random.sample(terms, min(40, len(terms)))

    for t in sample:
        entry_full = lex[t]
        docids_full, _ = reader.read_postings(entry_full)
        if not docids_full:
            continue

        first, last = docids_full[0], docids_full[-1]
        targets = [first-1, first, (first+last)//2, last, last+1]

        entry = dict(entry_full)
        entry.pop("blocks", None)

        for target in targets:
            hit = reader.seek_block_ge(entry, target)
            if target > last:
                assert hit is None
                continue
            assert hit is not None
            _, last_docid, d, f = hit
            assert last_docid == d[-1]
            assert last_docid >= target
            j = _lower_bound(d, target)
            if j < len(d):
                assert d[j] >= target
            if j > 0:
                assert d[j-1] < target

    reader.close()
