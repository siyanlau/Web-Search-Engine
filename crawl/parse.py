from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, urlunparse, urldefrag, parse_qsl, urlencode

class LinkExtractor(HTMLParser):
    def __init__(self, base_url):
        super().__init__()
        # Default base URL = page URL, unless overridden by <base>
        self.base_url = base_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        attrs = dict(attrs)

        # Capture <base href="..."> tag (only the first one counts)
        if tag == "base" and "href" in attrs and self.base_url == "":
            self.base_url = attrs["href"]

        # Capture <a href="..."> hyperlinks
        if tag == "a" and "href" in attrs:
            href = attrs["href"]
            abs_url = urljoin(self.base_url, href)  # resolve relative links
            canon = canonicalize_url(abs_url)
            if canon:
                self.links.append(canon)


def canonicalize_url(url):
    """
    Normalize and canonicalize a URL to reduce duplicates.

    Steps:
    - Remove URL fragment (#...).
    - Lowercase scheme and hostname.
    - Drop default ports (:80 for http, :443 for https).
    - Normalize index-like filenames (index.html, main.html, etc.) to directory root.
      NOTE: This is a heuristic – usually safe, but not guaranteed to be identical content.
    - Sort query parameters (except common trackers).
    - Remove empty path (collapse root to '/').
    """

    # Remove fragment
    url, _ = urldefrag(url)

    parsed = urlparse(url)

    # Lowercase scheme + hostname
    scheme = parsed.scheme.lower()
    netloc = parsed.hostname.lower() if parsed.hostname else ""

    # Add port if explicitly specified and not default
    if parsed.port:
        if (scheme == "http" and parsed.port != 80) or (scheme == "https" and parsed.port != 443):
            netloc = f"{netloc}:{parsed.port}"

    # Normalize path
    path = parsed.path or "/"

    # ---- Index file normalization ----
    # Map common "default" filenames to directory root.
    # e.g. http://site.com/foo/index.html → http://site.com/foo/
    index_suffixes = ("/index.html", "/index.htm", "/index.jsp", "/main.html")
    for suf in index_suffixes:
        if path.lower().endswith(suf):
            path = path[: -len(suf)] + "/"
            break

    # Normalize query: remove common tracking params, sort alphabetically
    query_params = []
    for k, v in parse_qsl(parsed.query, keep_blank_values=True):
        lk = k.lower()
        if lk.startswith("utm_") or lk in ("fbclid", "gclid"):
            continue
        query_params.append((lk, v))
    query_params.sort()
    query = urlencode(query_params)

    # Normalize empty path → '/'
    if path == "":
        path = "/"

    # Rebuild canonicalized URL
    canon = urlunparse((scheme, netloc, path, "", query, ""))
    return canon
