
import argparse, sys
from .crawler import crawl

def parse_args(argv):
    ap = argparse.ArgumentParser(description="MiniCrawler v0.2 (status-only)")
    ap.add_argument("--seeds-file", required=True, help="Path to newline-separated seed URLs")
    ap.add_argument("--out", default="crawl.csv", help="Output CSV path")
    ap.add_argument("--max-pages", type=int, default=50, help="Max number of fetch attempts")
    ap.add_argument("--max-depth", type=int, default=1, help="Max BFS depth to enqueue")
    ap.add_argument("--timeout", type=float, default=3.0, help="HTTP timeout seconds")
    ap.add_argument("--user-agent", default="MiniCrawler/0.2-status", help="User-Agent header")
    return ap.parse_args(argv)

def load_seeds(path):
    with open(path, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    seeds = load_seeds(args.seeds_file)
    crawl(seeds, args.out, args.max_pages, args.max_depth, args.timeout, args.user_agent)

if __name__ == "__main__":
    main()
