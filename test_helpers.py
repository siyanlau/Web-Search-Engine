from crawl.helpers import get_domain, get_superdomain

def run_tests():
    test_urls = [
        "http://cs.nyu.edu/index.html",
        "https://www.guardian.co.uk/news",
        "http://subdomain.example.com/page",
        "https://localhost:8080/",
        "http://weirdtld.technology/path",
        "http://bbc.co.uk",
        "http://ox.ac.uk",
    ]

    for url in test_urls:
        d = get_domain(url)
        s = get_superdomain(url)
        print(f"URL: {url}")
        print(f"  Domain:      {d}")
        print(f"  Superdomain: {s}")
        print("-" * 40)

if __name__ == "__main__":
    run_tests()
