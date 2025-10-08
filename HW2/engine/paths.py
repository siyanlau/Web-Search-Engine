# engine/paths.py

import os

DATA_DIR = "data"
DOC_LENGTHS_PATH = os.path.join(DATA_DIR, "doc_lengths.pkl")
INDEX_PATH = os.path.join(DATA_DIR, "intermediate_index.pkl")
LEXICON_PATH = os.path.join(DATA_DIR, "lexicon.pkl")
BLOCK_META_PATH = os.path.join(DATA_DIR, "block_meta.pkl")
MARCO_TSV_PATH = os.path.join(DATA_DIR, "marco_tiny.tsv")

os.makedirs(DATA_DIR, exist_ok=True)
