from robots import RobotCache

def test_urls():
    rc = RobotCache("MiniCrawlerTest/0.2", timeout=5)

    urls = [
        "https://en.wikipedia.org/wiki/Cat",
        "https://en.wikipedia.org/wiki/Wikipedia_talk:Administrator_elections",
        "https://wikipedia.org/wiki/Dog",
        "https://github.com/",
        "https://github.com/openai/gpt-3"
    ]

    for u in urls:
        allowed = rc.can_fetch(u)
        print(f"robots.can_fetch({u}) → {allowed}")

if __name__ == "__main__":
    test_urls()
