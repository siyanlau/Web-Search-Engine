v0.2 Ugly MVP Done
- pipeline: parse_docs() → build_inverted_index() → simple_search()
- todos:
    - compress.py
    - merge.py
- refactor plan (to accommodate later complexity):
    - add stats.py to handle cross-cutting concerns
    - turn naked scripts into classes for better encapsulation
        
Tentative Roadmap:
- v0.3 – Scoring Layer (BM25)
- v0.4 – Skip / Merge
- v0.5 – Compression Layer
- v0.6 – Disk Index

