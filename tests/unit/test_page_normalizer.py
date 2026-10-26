import pytest

from core.exceptions import InvalidValueError
from core.services.page_normalizer import PageNormalizer


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("https://user@site.ru:8443/horses/1?x=1", "/horses/1?x=1"),
        ("HTTP://site.ru", "/"),
        ("//user@site.ru:8080/horses/1", "/horses/1"),
        ("horses/1", "/horses/1"),
        ("https://site.ru?x=1#top", "/?x=1#top"),
        (" /Horses/ ", "/Horses/"),
        ("/%D0%BB%D0%BE%D1%88%D0%B0%D0%B4%D0%B8", "/лошади"),
        ("/лошади", "/лошади"),
        ("/a%2fb", "/a%2Fb"),
        ("/a%3fb%23c%25", "/a%3Fb%23c%25"),
        ("/a b\u2003c", "/a%20b%E2%80%83c"),
        ("/a\tb\nc", "/a%09b%0Ac"),
        ("/%01%7f", "/%01%7F"),
        ("/%zz%2f", "/%zz%2F"),
        ("/%ff%ab", "/%FF%AB"),
        ("/a?b=2&a=1", "/a?a=1&b=2"),
        ("/a?tag=x&b=1&tag=y", "/a?b=1&tag=x&tag=y"),
        ("/a?flag", "/a?flag="),
        ("/a?flag=", "/a?flag="),
        ("/a?=drop&x=1", "/a?x=1"),
        ("/a?q=a+b&q=a%2bb", "/a?q=a%20b&q=a%2Bb"),
        ("/a?%D1%8F=%D0%B0%26%3D%3F%23%25%2F", "/a?я=а%26%3D%3F%23%25%2F"),
        ("/a#", "/a"),
        ("/a#reviews", "/a#reviews"),
        ("/a#%D0%BE%D1%82%D0%B7%D1%8B%D0%B2%D1%8B", "/a#отзывы"),
        ("/a#x%2fy%23z", "/a#x%2Fy%23z"),
        ("/a#%FF", "/a#�"),
        ("/" + "а" * 10_000, "/" + "а" * 10_000),
    ],
)
def test_normalize(raw: str, expected: str) -> None:
    assert PageNormalizer().normalize(raw, blacklisted_params=frozenset(), blacklisted_hashes=frozenset()) == expected


@pytest.mark.parametrize("raw", ["", " \t\n ", "mailto:info@site.ru", "javascript:alert(1)", "https://["])
def test_invalid(raw: str) -> None:
    with pytest.raises(InvalidValueError, match="^Некорректное значение$"):
        PageNormalizer().normalize(raw, blacklisted_params=frozenset(), blacklisted_hashes=frozenset())


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("/a?utm_source=ya&id=5", "/a?id=5"),
        ("/a?%75tm_source=ya", "/a"),
        ("/a?utm_source=1&utm_medium=2", "/a"),
        ("/a?UTM_SOURCE=1", "/a?UTM_SOURCE=1"),
        ("/a?x=1#top", "/a?x=1"),
        ("/a#%74op", "/a"),
        ("/a#TOP", "/a#TOP"),
        ("/a#other", "/a#other"),
    ],
)
def test_blacklists(raw: str, expected: str) -> None:
    result = PageNormalizer().normalize(
        raw,
        blacklisted_params=frozenset({"utm_source", "utm_medium"}),
        blacklisted_hashes=frozenset({"top"}),
    )
    assert result == expected
