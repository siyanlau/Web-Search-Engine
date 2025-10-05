# engine/searcher.py
from engine.indexer import build_inverted_index
from engine.parser import parse_docs

def simple_search(query: str, index: dict[str, dict[int, int]], mode="AND"):
    q_terms = query.lower().split()
    postings = [set(index[t].keys()) for t in q_terms if t in index]
    if not postings:
        return set()
    if mode == "AND":
        return set.intersection(*postings)
    elif mode == "OR":
        return set.union(*postings)
    else:
        raise ValueError("mode must be AND or OR")

if __name__ == "__main__":
    docs, _ = parse_docs("data/marco_tiny.tsv", limit=100)
    index = build_inverted_index(docs)
    result = simple_search("presence", index, mode="AND")
    print(result)
