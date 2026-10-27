import re
from collections.abc import Collection

from core.exceptions import ParseProviderUnknownError
from core.protocols.parsing.provider import ParseProviderProtocol


class ParseProviderRegistry:
    def __init__(self, *, enabled_codes: Collection[str]) -> None:
        self._enabled_codes = tuple(enabled_codes)
        self._providers: dict[str, ParseProviderProtocol] = {}

    @property
    def registered_codes(self) -> tuple[str, ...]:
        return tuple(self._providers)

    def register(self, provider: ParseProviderProtocol) -> None:
        if re.fullmatch(r"[a-z][a-z0-9_]{1,31}", provider.code) is None:
            raise ValueError("Invalid parse provider code")
        if provider.subject != f"commands.parse.{provider.code}.run":
            raise ValueError("Invalid parse provider subject")
        if provider.code in self._providers:
            raise ValueError("Parse provider is already registered")
        self._providers[provider.code] = provider

    def get(self, code: str) -> ParseProviderProtocol:
        if code not in self._enabled_codes or code not in self._providers:
            raise ParseProviderUnknownError()
        return self._providers[code]

    def enabled(self) -> list[ParseProviderProtocol]:
        return [self.get(code) for code in self._enabled_codes]
