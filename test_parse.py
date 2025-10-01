# tests/test_parse.py
import unittest
from crawl.parse import LinkExtractor

HTML = """
<!doctype html>
<html>
<head>
  <base href="https://example.com/dir/#ignored-frag">
  <title>Test</title>
</head>
<body>
  <!-- normal relative -->
  <a href="a.html">A</a>
  <!-- relative with fragment -->
  <a href="b.html#section">B</a>
  <!-- absolute path -->
  <a href="/root">Root</a>
  <!-- absolute URL (http/https kept) -->
  <a href="https://example.com/abs/page?x=1#frag">Abs</a>

  <!-- should be skipped -->
  <a href="#local">Local anchor only</a>
  <a href="mailto:someone@example.com">Email</a>
  <a href="javascript:alert('x')">JS</a>
  <a href="tel:+12345678">Tel</a>
  <a href="ftp://example.com/file.txt">FTP</a>
  <a href="file:///C:/Windows">File</a>
  <a href="data:text/plain,hello">Data</a>
  <a href="blob:https://example.com/uuid">Blob</a>

  <!-- ensure only the FIRST <base> is used -->
  <base href="https://evil.example.org/should-not-apply/">
  <a href="c.html">C</a>

  <!-- HTML entity in URL should decode via convert_charrefs -->
  <a href="query?q=a&amp;b=1">Ent</a>
</body>
</html>
"""

class TestLinkExtractor(unittest.TestCase):
    def test_link_extraction_with_base_and_skips(self):
        p = LinkExtractor("https://fallback.example.com/page.html")
        p.feed(HTML)

        # What we expect (fragments removed, resolved against FIRST <base>)
        expected = {
            "https://example.com/dir/a.html",
            "https://example.com/dir/b.html",           # defrag removed
            "https://example.com/root",
            "https://example.com/abs/page?x=1",         # defrag removed
            "https://example.com/dir/c.html",           # second <base> ignored
            "https://example.com/dir/query?q=a&b=1",    # &amp; decoded
        }

        self.assertEqual(set(p.links), expected)

    def test_idempotent_feed(self):
        # Feeding twice should just append again (no internal dedupe here);
        # caller (crawler) is responsible for dedup via visited set.
        p = LinkExtractor("https://example.com/base/")
        p.feed('<a href="x.html#f">X</a>')
        p.feed('<a href="x.html#f">X</a>')
        self.assertEqual(
            p.links,
            ["https://example.com/base/x.html", "https://example.com/base/x.html"]
        )

if __name__ == "__main__":
    unittest.main()
