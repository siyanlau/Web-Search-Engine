import pickle
import sys

# python inspect_pickle.py data/intermediate_index.pkl
# python inspect_pickle.py data/doc_lengths.pkl 5


def inspect_pickle(path, limit=10):
    """
    Loads and prints a summary of a pickle file's content.
    Args:
        path: str, path to the pickle file
        limit: int, how many items to print if the object is a dict/list
    """
    print(f"\n[Inspecting {path}]")
    with open(path, "rb") as f:
        obj = pickle.load(f)

    print(f"Type: {type(obj)}")

    # Handle dict preview
    if isinstance(obj, dict):
        print(f"Dict length: {len(obj)}")
        print("First items:")
        for i, (k, v) in enumerate(obj.items()):
            print(f"  {repr(k)}: {repr(v)}")
            if i + 1 >= limit:
                break
    # Handle list preview
    elif isinstance(obj, list):
        print(f"List length: {len(obj)}")
        print("First items:")
        for i, v in enumerate(obj):
            print(f"  {repr(v)}")
            if i + 1 >= limit:
                break
    # Fallback: just print
    else:
        print("Object preview:")
        print(repr(obj))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python inspect_pickle.py path/to/file.pkl [limit]")
        sys.exit(1)
    path = sys.argv[1]
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else 10
    inspect_pickle(path, limit=limit)
