
#!/usr/bin/env python3
"""
Minimal single-thread BFS web crawler (v0.1).

- Parses only <a href> links.
- CSV output with columns: ts_iso,url (final URL after redirects).
- No robots.txt, no concurrency, no fancy normalization (only join + strip fragment).

Usage:
    python crawler.py --seeds-file seeds.txt --out crawl.csv --max-pages 200 --max-depth 1 --timeout 5 --user-agent "MiniCrawler/0.1"
"""
import argparse
import csv
import sys
import time
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag
from urllib.request import Request, build_opener, HTTPRedirectHandler, HTTPSHandler, HTTPHandler
from urllib.error import URLError, HTTPError
from collections import deque

DEFAULT_UA = "MiniCrawler/0.1"

class LinkExtractor(HTMLParser):
    """Minimal HTML link extractor: only <a href>."""
    def __init__(self, base_url):
        super().__init__(convert_charrefs=True)
        self.base_url = base_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if not href:
            return
        # Ignore non-http(s) schemes
        if href.startswith("mailto:") or href.startswith("javascript:"):
            return
        # Resolve relative; strip fragments
        abs_url = urljoin(self.base_url, href)
        abs_url, _frag = urldefrag(abs_url)
        self.links.append(abs_url)

def fetch_url(url, timeout, ua):
    """Fetch a URL and return (final_url, content_type, body_bytes or None)."""
    opener = build_opener(HTTPRedirectHandler, HTTPHandler, HTTPSHandler)
    req = Request(url, headers={"User-Agent": ua})
    try:
        with opener.open(req, timeout=timeout) as resp:
            final_url = resp.geturl()
            ctype = resp.headers.get("Content-Type", "") or ""
            # Only parse text/html
            if "text/html" in ctype.lower():
                body = resp.read()  # v0.1: read whole thing; no size cap
            else:
                body = None
            return final_url, ctype, body, None
    except HTTPError as e:
        return getattr(e, "url", url), getattr(e, "headers", {}).get("Content-Type", ""), None, e
    except URLError as e:
        return url, "", None, e
    except Exception as e:
        return url, "", None, e

def crawl(seeds, out_csv, max_pages, max_depth, timeout, ua):
    visited = set()
    q = deque() # tuple of 3. (URL, depth, parent URL)
    for s in seeds:
        s = s.strip()
        if not s:
            continue
        # strip fragment in seeds
        s, _ = urldefrag(s)
        q.append((s, 0, None))

    # open CSV
    outfh = open(out_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(outfh)
    writer.writerow(["ts_iso", "url"])  # v0.1 schema

    fetched = 0 # set the counter
    try:
        while q and fetched < max_pages:
            url, depth, ref = q.popleft()
            final_url, ctype, body, err = fetch_url(url, timeout, ua)
            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            writer.writerow([ts_iso, final_url])
            fetched += 1

            if final_url in visited:
                continue
            visited.add(final_url)

            # Only parse HTML pages and enqueue children if depth limit allows
            if body and depth < max_depth:
                try:
                    parser = LinkExtractor(final_url)
                    # decode best-effort; fallback to latin-1 to avoid crashes
                    text = body.decode("utf-8", errors="replace")
                    parser.feed(text)
                    for child in parser.links:
                        if child not in visited:
                            q.append((child, depth + 1, final_url))
                except Exception:
                    # Parsing errors are non-fatal in v0.1
                    pass
    finally:
        outfh.close()

def parse_args(argv):
    ap = argparse.ArgumentParser(description="MiniCrawler v0.1 (barebones)")
    ap.add_argument("--seeds-file", required=True, help="Path to newline-separated seed URLs")
    ap.add_argument("--out", default="crawl.csv", help="Output CSV path")
    ap.add_argument("--max-pages", type=int, default=200, help="Max number of fetch attempts")
    ap.add_argument("--max-depth", type=int, default=1, help="Max BFS depth to enqueue")
    ap.add_argument("--timeout", type=float, default=5.0, help="HTTP timeout seconds")
    ap.add_argument("--user-agent", default=DEFAULT_UA, help="User-Agent header")
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
