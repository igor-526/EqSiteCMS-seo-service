import re
import unicodedata

from core.exceptions import InvalidValueError


class SearchQueryNormalizer:
    def normalize(self, raw: str) -> str:
        value = re.sub(r"\s+", " ", unicodedata.normalize("NFC", raw).strip())
        if not value:
            raise InvalidValueError()
        return value[:1].upper() + value[1:].lower()
