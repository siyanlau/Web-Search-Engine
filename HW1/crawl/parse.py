from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag, urlparse, urlunparse, parse_qsl, urlencode

def canonicalize_url(url: str) -> str: 
    """
    Normalize URLs to reduce duplicates.
    - Lowercase scheme and host
    - Remove fragments
    - Remove default ports (:80 for http, :443 for https)
    - Strip common tracking query params (utm_*, fbclid, gclid) 
    - Sort query parameters
    - Collapse trailing slash for root
    """
    url, _ = urldefrag(url)  # drop fragment
    parsed = urlparse(url)

    scheme = parsed.scheme.lower()
    netloc = parsed.hostname.lower() if parsed.hostname else ""

    if parsed.port:
        if (scheme == "http" and parsed.port == 80) or (scheme == "https" and parsed.port == 443):
            pass  # default port, skip
        else:
            netloc = f"{netloc}:{parsed.port}"

    # Clean query
    query_pairs = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        if k.lower().startswith("utm_"):
            continue
        if k.lower() in ("fbclid", "gclid"):
            continue
        query_pairs.append((k, v))
    query_pairs.sort()
    clean_query = urlencode(query_pairs)
    
    # Normalize path
    path = parsed.path or ""

    # ---- Index file normalization ----
    # Map common "default" filenames to directory root.
    # e.g. http://site.com/foo/index.html → http://site.com/foo/
    index_suffixes = ("/index.html", "/index.htm", "/index.jsp", "/main.html")
    for suf in index_suffixes:
        if path.lower().endswith(suf):
            path = path[: -len(suf)] + "/"
            break

    # Collapse root path slash
    path = parsed.path or ""
    if path == "/":
        path = "" 
        
    rebuilt = urlunparse((scheme, netloc, path, "", clean_query, ""))
    return rebuilt

class LinkExtractor(HTMLParser):
    def __init__(self, base_url: str):
        super().__init__(convert_charrefs=True)
        self.links = []
        self.base_url = base_url
        self._base_seen = False

    def handle_starttag(self, tag, attrs):
        t = tag.lower()
        if t == "a":
            href = dict(attrs).get("href")
            if not href:
                return
            href = href.strip()
            # skip non-navigational schemes
            skip_schemes = ("mailto:", "javascript:", "tel:", "ftp:", "file:", "data:", "blob:")
            if href.startswith(skip_schemes) or href.startswith("#"):
                return
            abs_url = urljoin(self.base_url, href)
            abs_url = canonicalize_url(abs_url)
            self.links.append(abs_url)

        elif t == "base" and not self._base_seen:
            href = dict(attrs).get("href")
            if not href:
                return
            href = href.strip()
            new_base = urljoin(self.base_url, href)
            new_base = canonicalize_url(new_base)
            self.base_url = new_base
            self._base_seen = True
