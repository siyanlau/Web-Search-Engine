# engine/indexer.py
from collections import defaultdict
from engine.parser import parse_docs

def build_inverted_index(docs: dict[int, list[str]]):
    index = defaultdict(lambda: defaultdict(int))
    for docid, tokens in docs.items():
        for t in tokens:
            index[t][docid] += 1
    return index

if __name__ == "__main__":
    docs, _ = parse_docs("data/marco_tiny.tsv", limit=100)
    index = build_inverted_index(docs)
    print(f"Indexed {len(docs)} docs, {len(index)} terms")
    # sample output
    for term, postings in list(index.items())[:10]:
        print(term, list(postings.items())[:50])
