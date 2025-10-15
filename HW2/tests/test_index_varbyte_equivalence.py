# tests/test_index_varbyte_equivalence.py
import os, random
from engine.listio import ListWriter, ListReader

RANDOM_SEED = 2025

def test_varbyte_equals_raw(tmp_path):
    # Synthetic postings for multiple terms
    terms = {
        "apple": {1: 2, 5: 1, 12: 3, 130: 1, 131: 1},
        "banana": {3: 1, 4: 1, 10: 2, 1024: 7},
        "carrot": {2: 1},
        "delta": {100: 1, 200: 2, 5000: 1, 7000: 5, 9000: 1},
    }

    # Raw index
    raw_p = tmp_path / "raw.postings"
    w_raw = ListWriter(str(raw_p), block_size=3, codec="raw")
    entries_raw = {t: w_raw.add_term(t, postings) for t, postings in terms.items()}
    w_raw.close()

    # VarByte index
    vb_p = tmp_path / "vb.postings"
    w_vb = ListWriter(str(vb_p), block_size=3, codec="varbyte")
    entries_vb = {t: w_vb.add_term(t, postings) for t, postings in terms.items()}
    w_vb.close()

    # Compare term by term
    r_raw = ListReader(str(raw_p), codec="auto")
    r_vb  = ListReader(str(vb_p), codec="auto")

    for t in terms.keys():
        d1, f1 = r_raw.read_postings(entries_raw[t])
        d2, f2 = r_vb.read_postings(entries_vb[t])
        assert d1 == d2, f"docids mismatch for term={t}"
        assert f1 == f2, f"freqs mismatch for term={t}"

    r_raw.close()
    r_vb.close()
