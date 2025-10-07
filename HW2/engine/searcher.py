from engine.indexer import Indexer
from engine.parser import Parser
from engine.ranker import Ranker

class Searcher:
    """
    Simple in-memory searcher.
    Supports AND/OR search on in-memory inverted index.
    """
        
    def __init__(self, index, doc_lengths=None):
        self.index = index # term -> {docid: freq}
        self.ranker = None
        if doc_lengths:
            self.ranker = Ranker(index, doc_lengths)
        
    def search(self, query, mode="AND", topk=None):
        """
        Perform AND/OR search on index.
        query: string (space-separated terms)
        mode: "AND" or "OR"
        Returns set of docids.
        """
        if self.ranker:
            # BM25 ranking mode
            scores = self.ranker.score(query)
            return scores[:topk] if topk else scores
        else:
            # fallback: boolean mode
            q_terms = query.lower().split()
            postings = [set(self.index[t].keys()) for t in q_terms if t in self.index]
            if not postings:
                return set()
            if mode == "AND":
                return set.intersection(*postings)
            elif mode == "OR":
                return set.union(*postings)
            else:
                raise ValueError("mode must be AND or OR")

if __name__ == "__main__":
    # parser = Parser()
    # docs, _ = parser.parse_docs("data/marco_tiny.tsv")
    # indexer = Indexer()
    # index = indexer.build_inverted_index(docs)
    # searcher = Searcher(index)
    # result = searcher.search("presence", mode="AND")
    # print(result)
    if __name__ == "__main__":
        parser = Parser()
        docs, lens = parser.parse_docs("data/marco_tiny.tsv", limit=1000)
        indexer = Indexer()
        index = indexer.build_inverted_index(docs)

        searcher = Searcher(index, lens)
        result = searcher.search("important", topk=10)
        for docid, score in result:
            print(docid, round(score, 3))
