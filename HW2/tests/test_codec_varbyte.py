# tests/test_codec_varbyte.py
import os
from engine.listio import ListWriter, ListReader
from engine.lexicon import Lexicon

def test_varbyte_roundtrip(tmp_path):
    p = tmp_path / "vb.postings"
    lex = Lexicon()

    # Construct postings that span multiple blocks (block_size=4)
    postings = {
        1: 2,    # tf
        3: 1,
        130: 5,
        260: 1,  # end of block 1
        500: 2,  # block 2 starts; big gap to test varbyte and base
        501: 1,
        10000: 3,
        10050: 1,
    }

    # Write with varbyte
    w = ListWriter(str(p), block_size=4, codec="varbyte")
    entry = w.add_term("t", postings)
    w.close()

    # Sanity: lexicon entry has codec
    assert entry.get("codec") == "varbyte"
    assert entry["nblocks"] == 2

    # Read back
    r = ListReader(str(p), codec="auto")
    docids, freqs = r.read_postings(entry)
    r.close()

    assert list(postings.keys()) == docids
    assert list(postings.values()) == freqs
