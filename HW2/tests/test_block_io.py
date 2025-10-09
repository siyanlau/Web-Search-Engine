
"""
test_block_io.py

Internal consistency checks for v0.4 blocked index (no compression).

What it verifies:
  1) For randomly sampled terms from the lexicon:
     - Concatenated postings length equals DF in lexicon
     - DocIDs inside each block are strictly increasing
     - The last docID stored in block header equals the actual last docID
     - DocIDs across blocks are globally non-decreasing
  2) Reports basic corpus stats (terms, total postings, avg postings per term)

Run:
  python test_block_io.py --samples 200
"""

import argparse
import random
import sys

try:
    from engine.paths import LEXICON_PATH, POSTINGS_PATH
    from engine.lexicon import Lexicon
    from engine.listio import ListReader
except Exception as e:
    print("Import error. Run from project root so `engine` is importable.")
    raise

def check_blocks(reader, entry):
    """
    Read all blocks for a term using reader.read_postings(entry) and validate:
    - df matches total postings
    - header last_docid matches last doc id in block
    - docids are strictly increasing within each block
    - docids are non-decreasing across blocks
    """
    docids, freqs = reader.read_postings(entry)
    assert len(docids) == entry["df"], f"DF mismatch: df={entry['df']} vs read={len(docids)}"
    # per-block validation by re-reading sequentially
    # We mimic the reader's loop
    f = reader.file
    f.seek(entry["offset"])
    prev_last = -1
    remaining = entry["df"]
    while remaining > 0:
        import struct
        n, last_docid = struct.unpack("<II", f.read(8))
        d = struct.unpack(f"<{n}I", f.read(4*n))
        _ = f.read(4*n)  # skip freqs for ordering checks
        assert len(d) == n, "Block read error: docids length mismatch"
        # inside block strictly increasing
        for i in range(1, n):
            assert d[i] > d[i-1], "DocIDs must be strictly increasing within a block"
        # header matches
        assert d[-1] == last_docid, "Block header last_docid mismatch"
        # across blocks non-decreasing
        if prev_last != -1:
            assert d[0] > prev_last, "First docid of block must be greater than previous block's last"
        prev_last = last_docid
        remaining -= n
    return True

def main(samples):
    lex = Lexicon.load(LEXICON_PATH).map
    reader = ListReader(POSTINGS_PATH)

    terms = list(lex.keys())
    if not terms:
        print("Lexicon is empty.")
        return 1

    random.seed(42)
    sample_terms = random.sample(terms, min(samples, len(terms)))

    bad = 0
    total_postings = 0
    for t in sample_terms:
        entry = lex[t]
        total_postings += entry["df"]
        try:
            check_blocks(reader, entry)
        except AssertionError as e:
            bad += 1
            print(f"[FAIL] term='{t}': {e}")

    reader.close()
    ok = len(sample_terms) - bad
    avg_df = total_postings / max(1, len(sample_terms))
    print(f"Checked {len(sample_terms)} terms: OK={ok}, FAIL={bad}, avg_df={avg_df:.2f}")
    return 0 if bad == 0 else 2

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--samples", type=int, default=200, help="number of random terms to validate")
    args = ap.parse_args()
    sys.exit(main(args.samples))
