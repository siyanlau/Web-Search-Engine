import csv
import time
import random
import math
import heapq
from urllib.parse import urldefrag, urlparse

from .fetch import fetch_url
from .parse import LinkExtractor
from .robots import RobotCache
from .helpers import get_domain, get_superdomain

# enqueue-time gating for obvious binaries/static assets
BINARY_SUFFIXES = (
    ".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".ico",
    ".pdf", ".zip", ".tar", ".gz", ".tgz", ".bz2", ".xz", ".rar", ".7z",
    ".mp3", ".wav", ".flac", ".mp4", ".avi", ".mov", ".mkv", ".webm",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot"
)

def _looks_binary_by_suffix(url: str) -> bool:
    path = urlparse(url).path.lower()
    return any(path.endswith(ext) for ext in BINARY_SUFFIXES)

def _compute_priority(domain_before: int, super_before: int, super_w: float = 0.1):
    """
    Priority formula (higher = better).
    page_score   = 1 / log2(2 + domain_before)
    super_score  = super_w / log2(2 + super_before)
    """
    page_score = 1.0 / math.log2(2.0 + float(domain_before))
    super_score = super_w / math.log2(2.0 + float(super_before))
    total_priority = page_score + super_score
    return page_score, super_score, total_priority

def crawl(seeds, out_csv, max_pages, max_depth, timeout, ua):
    visited = set()
    in_frontier = set()

    pages_per_domain = {}
    pages_per_superdomain = {}

    robots = RobotCache(user_agent=ua, timeout=timeout)

    # Priority frontier: max-heap simulated via negated priority
    frontier = []
    seq = 0

    # Seed enqueue
    for s in seeds:
        s = (s or "").strip()
        if not s:
            continue
        s, _ = urldefrag(s)
        if not robots.can_fetch(s):
            print(f"[SEED SKIP] robots disallow {s}")
            continue
        if s in visited or s in in_frontier:
            continue
        d = get_domain(s)
        sd = get_superdomain(s)
        d_before = pages_per_domain.get(d, 0)
        sd_before = pages_per_superdomain.get(sd, 0)
        _, _, prio = _compute_priority(d_before, sd_before)
        heapq.heappush(frontier, (-prio, 0, seq, s))
        in_frontier.add(s)
        print(f"[SEED] push depth=0 prio={prio:.3f} {s}")
        seq += 1

    outfh = open(out_csv, "w", newline="", encoding="utf-8")
    writer = csv.writer(outfh)
    writer.writerow([
        "ts_iso", "url", "status", "depth", "bytes",
        "domain", "superdomain",
        "domain_count_before", "super_count_before",
        "page_score", "super_score", "total_priority"
    ])

    fetched = 0
    try:
        while frontier and fetched < max_pages:
            neg_prio, depth, _, url = heapq.heappop(frontier)
            in_frontier.discard(url)

            res = fetch_url(url, timeout, ua)
            final_url = res["final_url"]
            status = res["status"]
            body = res["body"]

            # Skip if already visited
            if final_url in visited:
                print(f"[SKIP DUP] {final_url}")
                continue

            ts_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            size_bytes = len(body) if body else 0
            domain = get_domain(final_url)
            superdomain = get_superdomain(final_url)

            domain_before = pages_per_domain.get(domain, 0)
            super_before = pages_per_superdomain.get(superdomain, 0)

            page_score, super_score, total_priority = _compute_priority(domain_before, super_before)

            # Log everything (including errors)
            writer.writerow([
                ts_iso, final_url, status, depth, size_bytes,
                domain, superdomain,
                domain_before, super_before,
                f"{page_score:.3f}", f"{super_score:.3f}", f"{total_priority:.3f}"
            ])
            fetched += 1

            print(f"[FETCH] {status} {final_url} depth={depth} bytes={size_bytes} "
                  f"domain={domain}({domain_before}) super={superdomain}({super_before}) "
                  f"scores=({page_score:.3f},{super_score:.3f}) total={total_priority:.3f}")

            # Mark visited and increment counters
            visited.add(final_url)
            pages_per_domain[domain] = domain_before + 1
            pages_per_superdomain[superdomain] = super_before + 1

            # Only parse children if status is normal and body exists and depth < max_depth
            if (not body) or (depth >= max_depth) or (status > 400):
                continue

            try:
                parser = LinkExtractor(final_url)
                text = body.decode("utf-8", errors="replace")
                print(f"[DEBUG] first 200 chars of body:\n{text[:200]!r}")
                parser.feed(text)
                links = parser.links
                print(f"[PARSE] found {len(links)} links at {final_url}")

                # Step 1: oversample + cap
                max_keep = 100
                oversample = 200
                original_n = len(links)
                if original_n > max_keep:
                    sample_idx = random.sample(range(original_n), min(oversample, original_n))
                    links = [links[i] for i in sample_idx]
                    print(f"[CAP] page had {original_n} links → sampled {len(links)} candidates")

                # Step 2: suffix filter
                filtered = []
                for u in links:
                    if _looks_binary_by_suffix(u):
                        print(f"[SKIP BIN] {u}")
                        continue
                    filtered.append(u)

                # Step 3: robots + enqueue children with scores
                accepted = 0
                for child in filtered:
                    if child in visited or child in in_frontier:
                        continue
                    if not robots.can_fetch(child):
                        print(f"[ROBOTS] disallow {child}")
                        continue
                    cd = get_domain(child)
                    csd = get_superdomain(child)
                    cd_before = pages_per_domain.get(cd, 0)
                    csd_before = pages_per_superdomain.get(csd, 0)
                    cp, sp, tp = _compute_priority(cd_before, csd_before)
                    heapq.heappush(frontier, (-tp, depth + 1, seq, child))
                    in_frontier.add(child)
                    seq += 1
                    accepted += 1
                    # print(f"[PUSH] depth={depth+1} prio={tp:.3f} {child}")
                    if accepted >= max_keep:
                        break

                print(f"[ENQUEUE] accepted={accepted}, frontier_size={len(frontier)}")

            except Exception as e:
                print(f"[PARSE ERROR] {e}")

    finally:
        outfh.close()
