
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag

class LinkExtractor(HTMLParser):
    """HTML link extractor with <base> support (anchors only)."""
    def __init__(self, page_url: str):
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.base_href = None
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "base" and self.base_href is None:
            href = None
            for k, v in attrs:
                if k.lower() == "href":
                    href = v
                    break
            if href:
                abs_base = urljoin(self.page_url, href)
                abs_base, _ = urldefrag(abs_base)
                self.base_href = abs_base
            return

        if tag != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if not href:
            return
        if href.startswith("mailto:") or href.startswith("javascript:"):
            return
        base = self.base_href or self.page_url
        abs_url = urljoin(base, href)
        abs_url, _ = urldefrag(abs_url)
        self.links.append(abs_url)
