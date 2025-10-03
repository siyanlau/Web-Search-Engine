from crawl.parse import LinkExtractor, canonicalize_url

def run_tests():
    html = """
    <html>
      <head>
        <base href="https://GitHub.com/SomePage">
      </head>
      <body>
        <a href="https://github.com/">Home</a>
        <a href="https://github.com?utm_source=foo&z=1&a=2">Tracking</a>
        <a href="/Contact#team">Contact</a>
        <a href="HTTP://EXAMPLE.com:80/path/">Port test</a>
        <a href="https://example.com:443/">Default https</a>
        <a href="https://example.com:8080/path">Non-default port</a>
        <a href="javascript:void(0)">Bad link</a>
      </body>
    </html>
    """
    extractor = LinkExtractor("https://github.com/")
    extractor.feed(html)

    print("Extracted Links:")
    for link in extractor.links:
        print(" ", link)

    # Test canonicalize_url separately
    print("\nCanonicalization Tests:")
    urls = [
        "https://GitHub.com/",
        "https://github.com?utm_source=foo&b=2&a=1",
        "https://example.com:443/",
        "http://example.com:80/",
        "https://example.com:8080/path",
        "https://example.com/page#fragment",
    ]
    for u in urls:
        print(f"{u}  -->  {canonicalize_url(u)}")

if __name__ == "__main__":
    run_tests()
