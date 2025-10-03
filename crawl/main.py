import argparse, sys
from .crawler import crawl
from .seed_from_query import get_seeds_from_query

def parse_args(argv):
    ap = argparse.ArgumentParser(description="MiniCrawler v0.5 with query mode")

    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--seeds-file", help="Path to newline-separated seed URLs")
    group.add_argument("--query", help="Search query to fetch seeds from DuckDuckGo")

    ap.add_argument("--out", default="crawl.csv", help="Output CSV path")
    ap.add_argument("--max-pages", type=int, default=50, help="Max number of fetch attempts")
    ap.add_argument("--max-depth", type=int, default=1, help="Max BFS depth to enqueue")
    ap.add_argument("--timeout", type=float, default=3.0, help="HTTP timeout seconds")
    ap.add_argument("--user-agent", default="MiniCrawler/0.5", help="User-Agent header")
    ap.add_argument("--num-seeds", type=int, default=10, help="Number of search results to use as seeds when using --query")

    return ap.parse_args(argv)

def load_seeds(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def main(argv=None):
    args = parse_args(argv or sys.argv[1:])

    if args.seeds_file:
        seeds = load_seeds(args.seeds_file)
    else:
        print(f"[QUERY] Fetching {args.num_seeds} seeds for query: {args.query}")
        seeds = get_seeds_from_query(args.query, num_results=args.num_seeds)

    crawl(seeds, args.out, args.max_pages, args.max_depth, args.timeout, args.user_agent)

if __name__ == "__main__":
    main()
