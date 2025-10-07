import pytest
from engine.parser import Parser
from engine.indexer import Indexer
from engine.searcher import Searcher

DATA_PATH = "data/toy.txt"

@pytest.fixture(scope="module")
def setup_engine():
    parser = Parser()
    docs, lens = parser.parse_docs(DATA_PATH)
    indexer = Indexer()
    index = indexer.build_inverted_index(docs)
    searcher = Searcher(index, lens)
    return searcher

def test_basic_score_positive(setup_engine):
    """All BM25 scores should be non-negative and non-zero for matched docs."""
    searcher = setup_engine
    result = searcher.search("coffee caffeine", topk=5)
    assert all(score >= 0 for _, score in result)
    assert any(score > 0 for _, score in result)

def test_relevant_doc_highest(setup_engine):
    """
    For clear queries, the most semantically relevant document should rank first.
    """
    searcher = setup_engine

    # (query, expected_top_docid)
    pairs = [
        ("coffee caffeine", 5),     # doc5 mentions both
        ("machine learning", 7),    # doc7 mentions exactly
        ("capital france", 2),      # doc2 mentions both
        ("great wall china", 4),    # doc4 mentions all three
        ("human brain neurons", 9), # doc9 direct mention
    ]

    for query, expected in pairs:
        results = searcher.search(query, topk=3)
        top_doc = results[0][0]
        print(f"{query:<25} → top doc {top_doc}, score={results[0][1]:.3f}")
        assert top_doc == expected, f"{query}: expected {expected}, got {top_doc}"

def test_query_with_overlap(setup_engine):
    """Query terms appearing in multiple docs should rank by term frequency / doc length."""
    searcher = setup_engine
    res = searcher.search("energy", topk=5)
    # photosynthesis (3) and coffee (5) both mention energy
    top_doc = res[0][0]
    assert top_doc in {3, 5}, "Energy-related docs (3,5) should be top-ranked"

def test_case_insensitive(setup_engine):
    """Ensure BM25 scoring is case-insensitive."""
    searcher = setup_engine
    q1 = searcher.search("Coffee", topk=3)
    q2 = searcher.search("coffee", topk=3)
    assert [d for d, _ in q1] == [d for d, _ in q2]

def test_no_match_returns_empty(setup_engine):
    """Nonexistent terms should yield empty result set."""
    searcher = setup_engine
    res = searcher.search("quantum entanglement", topk=3)
    assert len(res) == 0
