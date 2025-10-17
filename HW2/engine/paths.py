# engine/paths.py

import os

DATA_DIR = "data"

# --- Base data paths ---
DATA_DIR = "data"

# --- Intermediate pickle index (old version, still for testing) ---
INDEX_PATH = f"{DATA_DIR}/intermediate_index.pkl"

# --- New blocked index output files (v0.4) ---
POSTINGS_PATH = f"{DATA_DIR}/index2.postings"     # binary postings file
LEXICON_PATH = f"{DATA_DIR}/index2.lexicon"       # lexicon pickle file

# --- Document length mapping ---
DOC_LENGTHS_PATH = f"{DATA_DIR}/doc_lengths.pkl"

# --- Source corpus files ---
MARCO_TSV_PATH = os.path.join(DATA_DIR, "collection.tsv")

# --- Number of docs (for benchmarking) ---
NUM_DOCS = 1000000