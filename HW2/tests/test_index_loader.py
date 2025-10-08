from engine.index_loader import IndexLoader

def main():
    # The path should match the file you saved using Indexer.save_to_disk
    index_path = "data/intermediate_index.pkl"
    
    # Try loading the index
    print(f"Loading index from {index_path} ...")
    index = IndexLoader.load_index(index_path, format="pickle")
    print(f"Index loaded with {len(index)} terms.")

    # Do a simple posting lookup for a common term
    term = "communication"
    postings = index.get(term, {})
    print(f"Postings for term '{term}': {postings}")

    # Try printing postings for a rare/non-existent term
    term2 = "nonexistentterm"
    postings2 = index.get(term2, {})
    print(f"Postings for term '{term2}': {postings2}")

    # Optionally, show first 5 terms for sanity check
    print("\nFirst 5 terms in index (sample):")
    for i, (term, plist) in enumerate(index.items()):
        print(f"  {term}: {list(plist.items())[:5]}")
        if i >= 4: break

if __name__ == "__main__":
    main()
