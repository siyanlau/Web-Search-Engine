# engine/utils.py

import pickle

def write_doc_lengths(doc_lengths, path):
    """
    Save doc_lengths dictionary (docid -> length) to disk using pickle.
    Args:
        doc_lengths: dict[int, int]
        path: str, file path
    """
    with open(path, 'wb') as f:
        pickle.dump(doc_lengths, f)
    print(f"Doc lengths saved to {path}")

def load_doc_lengths(path):
    """
    Load doc_lengths dictionary from disk.
    Args:
        path: str, file path
    Returns:
        doc_lengths: dict[int, int]
    """
    with open(path, 'rb') as f:
        doc_lengths = pickle.load(f)
    print(f"Doc lengths loaded from {path}")
    return doc_lengths

def write_index(index, path):
    """
    Save inverted index dictionary (term -> {docid: freq}) to disk using pickle.
    Args:
        index: dict[str, dict[int, int]]
        path: str, file path
    """
    # Optional: convert defaultdict to dict for portability
    data = {term: dict(postings) for term, postings in index.items()}
    with open(path, 'wb') as f:
        pickle.dump(data, f)
    print(f"Inverted index saved to {path}")

def load_index(path):
    """
    Load inverted index dictionary from disk.
    Args:
        path: str, file path
    Returns:
        index: dict[str, dict[int, int]]
    """
    with open(path, 'rb') as f:
        index = pickle.load(f)
    print(f"Inverted index loaded from {path}")
    return index
