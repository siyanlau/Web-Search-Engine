from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag

class LinkExtractor(HTMLParser):
    def __init__(self, base_url: str):
        # convert_charrefs helps normalize &amp; etc.
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
            abs_url, _ = urldefrag(abs_url)  # optimize, we don't want to see the same url pop up multiple times because of different #fragments
            self.links.append(abs_url)

        elif t == "base" and not self._base_seen:
            href = dict(attrs).get("href")
            if not href:
                return
            href = href.strip()
            new_base = urljoin(self.base_url, href)
            new_base, _ = urldefrag(new_base)
            self.base_url = new_base
            self._base_seen = True
