
from html.parser import HTMLParser
from urllib.parse import urljoin, urldefrag

class LinkExtractor(HTMLParser):
    """Minimal HTML link extractor: anchors only, no <base> logic in this step."""
    def __init__(self, page_url: str):
        super().__init__(convert_charrefs=True)
        self.page_url = page_url
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag != "a":
            return
        href = None
        for k, v in attrs:
            if k.lower() == "href":
                href = v
                break
        if not href: # not a hyperlink
            return
        if href.startswith("mailto:") or href.startswith("javascript:"):
            return
        abs_url = urljoin(self.page_url, href)
        abs_url, _ = urldefrag(abs_url)
        self.links.append(abs_url)
