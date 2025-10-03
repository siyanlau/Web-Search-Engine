import csv, time, random
from collections import deque
from urllib.parse import urldefrag, urlparse

from .fetch import fetch_url
from .parse import LinkExtractor
from .robots import RobotCache
from .helpers import get_domain, get_superdomain

import math

# suffix blacklist (for enqueue-time gating)
BINARY_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".pdf", ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".rar", ".7z",
    ".mp3", ".wav", ".flac", ".mp4", ".avi", ".mov", ".mkv", ".webm",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot"
)

def _looks_binary_by_suffix(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_SUFFIXES)

def crawl(seeds, out_csv, max_pages, max_depth, timeout, ua):
    visited = set()
    q = deque()

    robots = RobotCache(user_agent=ua, timeout=timeout)

    # seed enqueue
    for s in seeds:
        s = s.strip()
        if not s:
            continue
        s, _ = urldefrag(s)
        if robots.can_fetch(s):
            q.append((s, 0))
        else:
            print(f"[SEED SKIP] robots disallow {s}")

    # counters for v0.3
    pages_per_domain = {}
    pages_per_superdomain = {}

    outfh = open(out_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(outfh)
    # extended header: add domain_count_before, super_count_before
    writer.writerow([
        "ts_iso", "url", "status", "depth", "bytes",
        "domain", "superdomain",
        "domain_count_before", "super_count_before",
        "page_score", "super_score", "total_priority"
    ])

    fetched = 0
    try:
        while q and fetched < max_pages:
            url, depth = q.popleft()
            res = fetch_url(url, timeout, ua)
            final_url = res["final_url"]
            status = res["status"]
            body = res["body"]
            
            # Early bailout if visited
            if final_url in visited:
                print(f"[SKIP DUP] {final_url}")
                continue

            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            size_bytes = len(body) if body else 0
            domain = get_domain(final_url)
            superdomain = get_superdomain(final_url)

            # counts BEFORE this visit (unique-page counts)
            domain_before = pages_per_domain.get(domain, 0)
            super_before = pages_per_superdomain.get(superdomain, 0)
            
            # compute scores
            page_score = 1.0 / math.log2(1 + (domain_before + 1))
            super_score = 0.1 / math.log2(1 + (super_before + 1))
            total_priority = page_score + super_score

            # log row includes counts + scores
            writer.writerow([
                ts_iso, final_url, status, depth, size_bytes,
                domain, superdomain,
                domain_before, super_before,
                f"{page_score:.3f}", f"{super_score:.3f}", f"{total_priority:.3f}"
            ])
            fetched += 1

            print(f"[FETCH] {status} {final_url} depth={depth}")

            # mark visited and increment unique counters
            visited.add(final_url)
            pages_per_domain[domain] = domain_before + 1
            pages_per_superdomain[superdomain] = super_before + 1

            # Only parse children if (a) body exists, (b) depth < max_depth, (c) status is 200
            # I want to keep 40x pages in the log cuz I get to know what exactly is getting crawled
            # However I don't want the CHILDREN of those error pages to be added to the queue
            if not body or depth >= max_depth or status >= 400:
                continue

            # parse + enqueue
            try:
                parser = LinkExtractor(final_url)
                text = body.decode("utf-8", errors="replace")
                # print(f"[DEBUG] first 200 chars of body:\n{text[:200]!r}")
                parser.feed(text)
                links = parser.links
                print(f"[PARSE] found {len(links)} links at {final_url}")

                # --- Step 1: cap with random oversampling ---
                max_keep = 100
                oversample = 200
                if len(links) > max_keep:
                    sample_idx = random.sample(
                        range(len(links)), min(oversample, len(links))
                    )
                    links = [links[i] for i in sample_idx]
                    print(f"[CAP] page had {len(parser.links)} links → sampled {len(links)} candidates")
                # --------------------------------------------

                # --- Step 2: filter suffixes only on sampled set ---
                filtered = []
                for u in links:
                    if _looks_binary_by_suffix(u):
                        print(f"[SKIP BIN] {u}")
                        continue
                    filtered.append(u)
                # ---------------------------------------------------

                # --- Step 3: robots + enqueue ---
                accepted = 0
                for child in filtered:
                    if child in visited:
                        continue

                    # # optional: same-host restriction for debug
                    # if urlparse(child).netloc != urlparse(final_url).netloc:
                    #     print(f"[SKIP EXT] {child}")
                    #     continue

                    allowed = robots.can_fetch(child)
                    if not allowed:
                        print(f"[ROBOTS] disallow {child}")
                        continue
                    q.append((child, depth + 1))
                    accepted += 1
                    if accepted >= max_keep:
                        break

                print(f"[ENQUEUE] accepted={accepted}, queue_size={len(q)}")

            except Exception as e:
                print(f"[PARSE ERROR] {e}")
    finally:
        outfh.close()