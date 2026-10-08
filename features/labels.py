"""
Corrosion severity from the Bi3Q3 YOLO boxes: the one labelling rule, used by the feature
table (training labels) and the committed frame library (truth for the synthetic videos).

  none     no corrosion box (crack or slippage boxes alone are not corrosion)
  severe   corrosion boxes cover >= 2 % of the frame, or there are >= 3 of them
  surface  any other corrosion

The plan proposed "area >= 15 %"; on Bi3Q3 that marks 4 of 1,249 images severe (corrosion
boxes are small: the 90th percentile of the per-image area is 7 %), so the rule was set on the
data in Phase 0 (docs/PROJECT_LOG.md). Standard library only.
"""
from __future__ import annotations

CLASSES = ("Slippage", "corrosion", "crack")
CORROSION = CLASSES.index("corrosion")
SEVERITIES = ("none", "surface", "severe")
SEVERE_AREA = 0.02
SEVERE_BOXES = 3


def corrosion_stats(yolo_text: str) -> tuple[float, int]:
    """(summed corrosion box area as a fraction of the frame, capped at 1; number of boxes)."""
    area, n = 0.0, 0
    for line in yolo_text.splitlines():
        p = line.split()
        if len(p) >= 5 and int(float(p[0])) == CORROSION:
            area += float(p[3]) * float(p[4])
            n += 1
    return min(area, 1.0), n


def severity(area: float, n_boxes: int) -> str:
    if n_boxes == 0:
        return "none"
    return "severe" if area >= SEVERE_AREA or n_boxes >= SEVERE_BOXES else "surface"


def severity_of(yolo_text: str) -> str:
    return severity(*corrosion_stats(yolo_text))
