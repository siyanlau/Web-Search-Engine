# tests/test_daat_cursor_edges.py
import random
import bisect
import pytest

from engine.paths import LEXICON_PATH, POSTINGS_PATH
from engine.lexicon import Lexicon
from engine.listio import ListReader
from engine.daat import PostingsCursor

RANDOM_SEED = 2026

def test_cursor_next_ge_edges():
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)
    terms = list(lex.keys())
    assert terms

    random.seed(RANDOM_SEED)
    # Pick some terms with various dfs
    sample = random.sample(terms, min(80, len(terms)))
    for t in sample:
        entry = lex[t]
        if entry["df"] == 0:
            continue
        # read full postings just for generating targets
        d_all, _ = reader.read_postings(entry)
        cur = PostingsCursor(reader, t, entry)
        assert cur.docid() == d_all[0]

        # targets: before first, equal to some, between, after last
        cand = {d_all[0]-1, d_all[0], d_all[-1], d_all[-1]+1}
        cand |= set(random.sample(d_all, min(3, len(d_all))))
        for target in sorted(cand):
            got = cur.next_ge(target)
            if target > d_all[-1]:
                assert got is None
                break
            j = bisect.bisect_left(d_all, target)
            assert got == d_all[j]

        # 重新创建一个新的 cursor，再验证 advance 遍历全表
        cur = PostingsCursor(reader, t, entry)
        seen = [cur.docid()]
        while True:
            nxt = cur.advance()
            if nxt is None:
                break
            seen.append(nxt)
        assert seen == d_all

    reader.close()
