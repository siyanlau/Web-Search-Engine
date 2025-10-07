import re
import pytest

TOKEN_REGEX = re.compile(r"[a-z0-9]+(?:[.-][a-z0-9]+)*", re.IGNORECASE)

def tokenize(text):
    return TOKEN_REGEX.findall(text.lower())

@pytest.mark.parametrize("text,expected", [
    ("U.S.", ["u.s"]),
    ("U.S.A.", ["u.s.a"]),
    ("COVID-19", ["covid-19"]),
    ("foo-bar-baz", ["foo-bar-baz"]),
    ("3.1415926", ["3.1415926"]),
    ("3.141.5926", ["3.141.5926"]),
    ("foo, bar.", ["foo", "bar"]),
    ("foo_bar", ["foo", "bar"]),
    ("foo--bar", ["foo", "bar"]),
    ("foo...bar", ["foo", "bar"]),
    ("E.U.", ["e.u"]),
    ("foo-bar,bar.baz", ["foo-bar", "bar.baz"]),
    ("abc! def? ghi...", ["abc", "def", "ghi"]),
    ("123", ["123"]),
    ("foo-123", ["foo-123"]),
    ("a...b", ["a", "b"]),
    ("---foo---bar---", ["foo", "bar"]),
    (".foo.", ["foo"]),
    ("...", []),
    ("e-mail", ["e-mail"]),
    ("COVID19", ["covid19"]),
    ("c3po", ["c3po"]),
    ("3.14e10", ["3.14e10"]),
    ("2023-10-06", ["2023-10-06"]),
])
def test_tokenizer(text, expected):
    assert tokenize(text) == expected
