"""Графики отчёта в SVG без внешних зависимостей: линии по времени и столбцы."""

from __future__ import annotations

import math
from dataclasses import dataclass
from html import escape
from typing import TYPE_CHECKING

from benchmarks.prose import prose

if TYPE_CHECKING:
    from collections.abc import Sequence

__all__ = ["BarChart", "LineChart", "Series", "render_svg"]

_WIDTH = 760
_HEIGHT = 360
_LEFT = 72
_RIGHT = 180
_TOP = 40
_BOTTOM = 48
_COLORS = ("#2a6fdb", "#e0662b", "#2e9e55", "#9b4dca", "#c23b4b", "#6b7280", "#d4a017")
_TICKS = 5
_THOUSANDS = 1000
"""С этого значения подпись оси — целое с разделителем тысяч."""
_INTEGERS = 10
"""С этого значения подпись оси — целое; меньше — три значащие цифры."""


@dataclass(frozen=True, slots=True)
class Series:
    """Одна линия графика: подпись и точки ``(x, y)``."""

    label: str
    points: tuple[tuple[float, float], ...]


@dataclass(frozen=True, slots=True)
class LineChart:
    """Линии по общей оси X (обычно секунды прогона); ``limit`` — горизонтальная цель."""

    name: str
    title: str
    x_label: str
    y_label: str
    series: tuple[Series, ...]
    limit: float | None = None


@dataclass(frozen=True, slots=True)
class BarChart:
    """Столбцы: подпись категории и значение."""

    name: str
    title: str
    y_label: str
    bars: tuple[tuple[str, float], ...]
    limit: float | None = None


def _finite(values: Sequence[float]) -> list[float]:
    return [value for value in values if math.isfinite(value)]


def _bounds(values: Sequence[float]) -> tuple[float, float]:
    finite = _finite(values)
    if not finite:
        return 0.0, 1.0
    low = min(0.0, *finite)
    high = max(finite)
    if high <= low:
        high = low + 1.0
    return low, high * 1.05


def _fmt(value: float) -> str:
    if value == 0 or not math.isfinite(value):
        return "0"
    magnitude = abs(value)
    if magnitude >= _THOUSANDS:
        return f"{value:,.0f}".replace(",", " ")
    if magnitude >= _INTEGERS:
        return f"{value:.0f}"
    return f"{value:.3g}"


def _frame(title: str, y_label: str, x_label: str) -> list[str]:
    plot_bottom = _HEIGHT - _BOTTOM
    return [
        (
            prose(
                f"""
                <svg xmlns="http://www.w3.org/2000/svg" width="{_WIDTH}" height="{_HEIGHT}"
                viewBox="0 0 {_WIDTH} {_HEIGHT}" font-family="sans-serif" font-size="12">
                """
            )
        ),
        f'<rect width="{_WIDTH}" height="{_HEIGHT}" fill="#ffffff"/>',
        (
            prose(
                f"""
                <text x="{_WIDTH / 2}" y="22" text-anchor="middle" font-size="14"
                font-weight="bold">{escape(title)}</text>
                """
            )
        ),
        (
            prose(
                f"""
                <line x1="{_LEFT}" y1="{plot_bottom}" x2="{_WIDTH - _RIGHT}" y2="{plot_bottom}"
                stroke="#333"/>
                """
            )
        ),
        f'<line x1="{_LEFT}" y1="{_TOP}" x2="{_LEFT}" y2="{plot_bottom}" stroke="#333"/>',
        (
            prose(
                f"""
                <text x="16" y="{(_TOP + plot_bottom) / 2}" text-anchor="middle"
                transform="rotate(-90 16 {(_TOP + plot_bottom) / 2})">{escape(y_label)}</text>
                """
            )
        ),
        (
            prose(
                f"""
                <text x="{(_LEFT + _WIDTH - _RIGHT) / 2}" y="{_HEIGHT - 10}"
                text-anchor="middle">{escape(x_label)}</text>
                """
            )
        ),
    ]


def _y_ticks(low: float, high: float) -> list[str]:
    plot_bottom = _HEIGHT - _BOTTOM
    parts: list[str] = []
    for index in range(_TICKS + 1):
        value = low + (high - low) * index / _TICKS
        y = plot_bottom - (plot_bottom - _TOP) * index / _TICKS
        parts.extend(
            (
                (
                    prose(
                        f"""
                        <line x1="{_LEFT}" y1="{y:.1f}" x2="{_WIDTH - _RIGHT}" y2="{y:.1f}"
                        stroke="#e5e7eb"/>
                        """
                    )
                ),
                f'<text x="{_LEFT - 6}" y="{y + 4:.1f}" text-anchor="end">{_fmt(value)}</text>',
            )
        )
    return parts


def _limit_line(limit: float | None, low: float, high: float) -> list[str]:
    if limit is None or not math.isfinite(limit):
        return []
    plot_bottom = _HEIGHT - _BOTTOM
    y = plot_bottom - (plot_bottom - _TOP) * (limit - low) / (high - low)
    return [
        (
            prose(
                f"""
                <line x1="{_LEFT}" y1="{y:.1f}" x2="{_WIDTH - _RIGHT}" y2="{y:.1f}" stroke="#c23b4b"
                stroke-dasharray="6 4"/>
                """
            )
        ),
        f'<text x="{_WIDTH - _RIGHT + 6}" y="{y + 4:.1f}" fill="#c23b4b">цель {_fmt(limit)}</text>',
    ]


def _point(
    x: float, y: float, *, x_range: tuple[float, float], y_range: tuple[float, float]
) -> str:
    plot_bottom = _HEIGHT - _BOTTOM
    plot_right = _WIDTH - _RIGHT
    px = _LEFT + (plot_right - _LEFT) * (x - x_range[0]) / (x_range[1] - x_range[0])
    py = plot_bottom - (plot_bottom - _TOP) * (y - y_range[0]) / (y_range[1] - y_range[0])
    return f"{px:.1f},{py:.1f}"


def _line_svg(chart: LineChart) -> str:
    xs = [x for series in chart.series for x, _ in series.points]
    ys = [y for series in chart.series for _, y in series.points]
    if chart.limit is not None:
        ys.append(chart.limit)
    x_low, x_high = _bounds(xs)
    y_low, y_high = _bounds(ys)
    plot_bottom = _HEIGHT - _BOTTOM
    plot_right = _WIDTH - _RIGHT
    parts = [*_frame(chart.title, chart.y_label, chart.x_label), *_y_ticks(y_low, y_high)]
    for index in range(_TICKS + 1):
        value = x_low + (x_high - x_low) * index / _TICKS
        x = _LEFT + (plot_right - _LEFT) * index / _TICKS
        parts.append(
            f'<text x="{x:.1f}" y="{plot_bottom + 16}" text-anchor="middle">{_fmt(value)}</text>'
        )
    for index, series in enumerate(chart.series):
        color = _COLORS[index % len(_COLORS)]
        coords = " ".join(
            _point(x, y, x_range=(x_low, x_high), y_range=(y_low, y_high))
            for x, y in series.points
            if math.isfinite(x) and math.isfinite(y)
        )
        if coords:
            parts.append(
                f'<polyline fill="none" stroke="{color}" stroke-width="2" points="{coords}"/>'
            )
        legend_y = _TOP + 16 * index
        parts.extend(
            (
                (
                    prose(
                        f"""
                        <rect x="{plot_right + 8}" y="{legend_y}" width="10" height="10"
                        fill="{color}"/>
                        """
                    )
                ),
                f'<text x="{plot_right + 24}" y="{legend_y + 9}">{escape(series.label)}</text>',
            )
        )
    parts.extend(_limit_line(chart.limit, y_low, y_high))
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def _bar_svg(chart: BarChart) -> str:
    values = [value for _, value in chart.bars]
    if chart.limit is not None:
        values.append(chart.limit)
    y_low, y_high = _bounds(values)
    plot_bottom = _HEIGHT - _BOTTOM
    plot_right = _WIDTH - _RIGHT
    parts = [*_frame(chart.title, chart.y_label, ""), *_y_ticks(y_low, y_high)]
    count = max(len(chart.bars), 1)
    slot = (plot_right - _LEFT) / count
    for index, (label, value) in enumerate(chart.bars):
        shown = value if math.isfinite(value) else 0.0
        top = plot_bottom - (plot_bottom - _TOP) * (shown - y_low) / (y_high - y_low)
        x = _LEFT + slot * index + slot * 0.15
        color = _COLORS[index % len(_COLORS)]
        parts.extend(
            (
                (
                    prose(
                        f"""
                        <rect x="{x:.1f}" y="{top:.1f}" width="{slot * 0.7:.1f}"
                        height="{plot_bottom - top:.1f}" fill="{color}"/>
                        """
                    )
                ),
                (
                    prose(
                        f"""
                        <text x="{x + slot * 0.35:.1f}" y="{top - 4:.1f}"
                        text-anchor="middle">{_fmt(value)}</text>
                        """
                    )
                ),
                (
                    prose(
                        f"""
                        <text x="{x + slot * 0.35:.1f}" y="{plot_bottom + 16}"
                        text-anchor="middle">{escape(label)}</text>
                        """
                    )
                ),
            )
        )
    parts.extend(_limit_line(chart.limit, y_low, y_high))
    parts.append("</svg>")
    return "\n".join(parts) + "\n"


def render_svg(chart: LineChart | BarChart) -> str:
    """SVG-документ графика.

    Returns:
        Текст SVG.
    """
    if isinstance(chart, LineChart):
        return _line_svg(chart)
    return _bar_svg(chart)
