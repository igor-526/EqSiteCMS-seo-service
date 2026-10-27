from decimal import ROUND_HALF_UP, Decimal


class MetricValueFormatter:
    def format(self, value: int | float | Decimal | None, value_type: str) -> str:
        if value is None:
            return "—"
        number = Decimal(str(value))
        if value_type == "integer":
            return f"{int(number.quantize(Decimal('1'), rounding=ROUND_HALF_UP)):,}".replace(",", "\u00a0")
        if value_type in {"decimal", "percent"}:
            text = format(number.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP), "f").rstrip("0").rstrip(".")
            return text.replace(".", ",") + ("%" if value_type == "percent" else "")
        if value_type == "duration":
            seconds = int(number.quantize(Decimal("1"), rounding=ROUND_HALF_UP))
            hours, rest = divmod(seconds, 3600)
            minutes, seconds = divmod(rest, 60)
            parts = [f"{n} {unit}" for n, unit in ((hours, "ч"), (minutes, "мин"), (seconds, "с")) if n]
            return " ".join(parts[:2]) or "0 с"
        return str(value)
