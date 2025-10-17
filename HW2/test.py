from engine.searcher import Searcher
import time

s = Searcher()                                  # loads lexicon/postings/doc_lengths
# print(s.search("machine learning", topk=10))     # BM25, OR by default
# print(s.search("communication policy", mode="AND", topk=10))  # Boolean AND
start_time = time.perf_counter()
results = s.search_topk_daat("machine learning", mode="AND")
end_time = time.perf_counter()
search_time = (end_time - start_time) * 1000  # Convert to milliseconds
print(results)
print(f"Search time: {search_time} milliseconds")