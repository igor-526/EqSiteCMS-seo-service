"""Local heartbeat probe. No network, credentials, HTTP endpoint or worker startup."""

from pathlib import Path
from time import time

from settings import NormalizationSettings


def is_ready(settings: NormalizationSettings) -> bool:
    if not settings.normalization_enabled:
        return False
    try:
        stamp = float(Path(settings.normalization_readiness_file).read_text())
        return 0 <= time() - stamp < 5
    except OSError, ValueError:
        return False


if __name__ == "__main__":
    raise SystemExit(0 if is_ready(NormalizationSettings()) else 1)
