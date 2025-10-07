# engine/ranker.py
import math
from collections import defaultdict
from engine.indexer import Indexer
from engine.parser import Parser

class Ranker:
    """
    BM25 ranker: computes document scores given an inverted index.
    """

    def __init__(self, index, doc_lengths, k1=1.2, b=0.75):
        self.index = index
        self.doc_lengths = doc_lengths
        self.k1 = k1
        self.b = b
        self.N = len(doc_lengths)
        self.df = {term: len(postings) for term, postings in index.items()}
        self.avgdl = sum(doc_lengths.values()) / self.N

    def bm25(self, tf, df, dl):
        """Compute BM25 score for a single term occurrence."""
        idf = math.log((self.N - df + 0.5) / (df + 0.5) + 1)
        numerator = tf * (self.k1 + 1)
        denominator = tf + self.k1 * (1 - self.b + self.b * dl / self.avgdl)
        return idf * numerator / denominator

    def score(self, query):
        """Compute BM25 scores for all docs matching query terms."""
        q_terms = query.lower().split()
        scores = defaultdict(float)
        for term in q_terms:
            if term not in self.index:
                continue
            df = self.df[term]
            for docid, tf in self.index[term].items():
                dl = self.doc_lengths[docid]
                scores[docid] += self.bm25(tf, df, dl)
        return sorted(scores.items(), key=lambda x: x[1], reverse=True)

if __name__ == "__main__":
    # --- quick sanity check ---
    data_path = "data/toy.txt"
    print(f"Loading docs from {data_path} ...")

    parser = Parser()
    docs, lens = parser.parse_docs(data_path)
    print(f"Loaded {len(docs)} docs.")

    indexer = Indexer()
    index = indexer.build_inverted_index(docs)
    print(f"Built index with {len(index)} terms.")

    ranker = Ranker(index, lens)

    queries = [
        "coffee caffeine",
        "machine learning",
        "capital france",
        "and",
        "that",
    ]

    for q in queries:
        results = ranker.score(q)
        print(f"\nQuery: {q}")
        for docid, score in results:
            print(f"  Doc {docid:>2}  Score={score:.3f}")