from crawl.seed_from_query import get_seeds_from_query

def test_seed_query():
    query = "Monkey"
    print(f"[TEST] Query: {query}")

    seeds = get_seeds_from_query(query, num_results=10)

    print(f"[RESULT] Got {len(seeds)} seeds")
    for i, url in enumerate(seeds, 1):
        print(f"{i:2d}. {url}")

if __name__ == "__main__":
    test_seed_query()
