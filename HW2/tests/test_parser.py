# tests/test_parser.py
import os
import re
from engine.parser import parse_docs

DATA_PATH = "data/toy.txt"

def test_file_exists():
    """Verify that toy.txt exists and is readable."""
    assert os.path.exists(DATA_PATH), f"{DATA_PATH} not found"
    with open(DATA_PATH, "r", encoding="utf8") as f:
        first_line = f.readline()
        assert first_line.strip(), "File is empty or unreadable"


def test_parse_docs_basic():
    """Basic sanity check: should load >=1 doc, each with non-empty tokens."""
    docs, lens = parse_docs(DATA_PATH)
    assert len(docs) > 0, "No documents loaded"
    assert len(lens) == len(docs), "doc_lengths size mismatch"

    # check key types
    assert all(isinstance(k, int) for k in docs.keys()), "docIDs must be int"

    # check token list type + non-empty
    for docid, toks in docs.items():
        assert isinstance(toks, list), f"Doc {docid} tokens should be list"
        assert all(isinstance(t, str) for t in toks), f"Non-string token in doc {docid}"
        assert all(t.islower() for t in toks), f"Token not lowercased in doc {docid}"
        assert len(toks) > 0, f"Doc {docid} has empty token list"
        assert lens[docid] == len(toks), f"Length mismatch for doc {docid}"

def test_tokenization_pattern():
    """Tokens should contain only a-z or digits."""
    docs, _ = parse_docs(DATA_PATH)
    pattern = re.compile(r"^[a-z0-9]+$")
    for docid, toks in docs.items():
        for t in toks:
            assert pattern.match(t), f"Invalid token '{t}' in doc {docid}"
