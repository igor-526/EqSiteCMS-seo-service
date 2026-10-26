import re
import unicodedata
from urllib.parse import parse_qsl, unquote, urlsplit

from core.exceptions import InvalidValueError

_PERCENT = re.compile(r"%[0-9a-fA-F]{2}")
_INVALID_PERCENT = re.compile(r"%(?![0-9a-fA-F]{2})")


def _encode(value: str, *, query: bool = False) -> str:
    reserved = "%/?#" + ("&=+" if query else "")
    return "".join(
        "".join(f"%{byte:02X}" for byte in char.encode("utf-8"))
        if char in reserved or char.isspace() or unicodedata.category(char) == "Cc"
        else char
        for char in value
    )


def _path_segment(segment: str) -> str:
    try:
        if _INVALID_PERCENT.search(segment):
            raise ValueError
        return _encode(unquote(segment, errors="strict"))
    except UnicodeDecodeError, ValueError:
        # Legacy encodings and malformed escapes must survive without losing data.
        return _PERCENT.sub(lambda match: match[0].upper(), segment)


class PageNormalizer:
    def normalize(
        self,
        raw: str,
        *,
        blacklisted_params: frozenset[str],
        blacklisted_hashes: frozenset[str],
    ) -> str:
        value = raw.strip()
        if not value:
            raise InvalidValueError()
        # urlsplit silently drops literal TAB/CR/LF; preserve them as escaped bytes.
        value = value.replace("\t", "%09").replace("\r", "%0D").replace("\n", "%0A")
        try:
            parts = urlsplit(value)
        except ValueError:
            raise InvalidValueError() from None
        if parts.scheme and parts.scheme.lower() not in {"http", "https"}:
            raise InvalidValueError()
        path = parts.path or "/"
        if not path.startswith("/"):
            path = "/" + path
        path = "/".join(_path_segment(segment) for segment in path.split("/"))
        pairs = [
            (name, content)
            for name, content in parse_qsl(parts.query, keep_blank_values=True)
            if name and name not in blacklisted_params
        ]
        query = "&".join(
            f"{_encode(name, query=True)}={_encode(content, query=True)}"
            for name, content in sorted(pairs, key=lambda pair: pair[0])
        )
        fragment = unquote(parts.fragment)
        if fragment in blacklisted_hashes:
            fragment = ""
        return path + ("?" + query if query else "") + ("#" + _encode(fragment) if fragment else "")
