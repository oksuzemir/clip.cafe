from __future__ import annotations

import json
import time
from pathlib import Path
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple

def decimal_ms(v: float) -> int:
    d = Decimal(str(v)).quantize(Decimal("0.001"), rounding=ROUND_HALF_UP)
    return int(d * 1000)

def float_to_srt_time(f: float) -> str:
    total_ms = decimal_ms(f)
    ms = total_ms % 1000
    total_s = total_ms // 1000
    s = total_s % 60
    total_m = total_s // 60
    m = total_m % 60
    h = total_m // 60
    return f"{h:02d}:{m:02d}:{s:02d},{ms:03d}"

def parse_subs_field(subs_field) -> list[tuple[float, float, str]]:
    if not subs_field:
        return []
    if isinstance(subs_field, str):
        try:
            data = json.loads(subs_field)
        except Exception:
            return []
    else:
        data = subs_field
    items = []
    try:
        keys = sorted(data.keys(), key=lambda k: int(k) if str(k).isdigit() else k)
    except Exception:
        keys = list(data.keys())
    for k in keys:
        entry = data[k]
        try:
            start = float(entry.get("TimeStart", 0))
        except:
            start = 0.0
        try:
            end = float(entry.get("TimeEnd", start + 2.0))
        except:
            end = start + 2.0
        text = entry.get("Text", "") or entry.get("text", "") or ""
        text = text.replace("\r\n", "\n").replace("\r", "\n").strip()
        items.append((start, end, text))
    return items

def write_srt(items: list[tuple[float, float, str]], out_path: Path) -> Path:
    lines = []
    for i, (start, end, text) in enumerate(items, start=1):
        lines.append(str(i))
        lines.append(f"{float_to_srt_time(start)} --> {float_to_srt_time(end)}")
        lines.extend(text.split("\n"))
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path

def generate_ass_from_srt(
    srt_path: Path,
    ass_path: Path,
    font: str,
    size: int,
    marginv: int,
    marginl: int,
    marginr: int,
    title_text: Optional[str] = None,
    title_size: Optional[int] = None,
    title_pos_y: Optional[int] = None,
    duration_s: Optional[float] = None
) -> Tuple[bool, Optional[str]]:
    """
    Convert SRT -> ASS and optionally embed a top-centered title (using absolute y coordinate).
    title_pos_y: if provided, will be used in an ASS override \pos(540, title_pos_y).
    Returns (True, ass_path) or (False, error_message).
    """
    try:
        import pysubs2
    except Exception as e:
        return False, f"pysubs2 not installed: {e}"
    try:
        subs = pysubs2.load(str(srt_path))
        # Default subtitle style (bottom-center)
        style = subs.styles.get("Default") or pysubs2.SSAStyle()
        style.fontname = font
        style.fontsize = size
        style.alignment = 2  # bottom-center
        style.marginv = marginv
        style.marginl = marginl
        style.marginr = marginr
        style.outline = style.outline if getattr(style, "outline", None) is not None else 1
        style.shadow = 0
        subs.styles["Default"] = style

        # Title style and event (if provided)
        if title_text:
            title_style = pysubs2.SSAStyle()
            title_style.fontname = font
            title_style.fontsize = title_size if title_size else size
            title_style.alignment = 8  # top-center (but we'll use \pos for precise placement)
            title_style.marginv = 0
            title_style.marginl = marginl
            title_style.marginr = marginr
            title_style.outline = 1
            title_style.shadow = 0
            subs.styles["Title"] = title_style

            end_ms = int(duration_s * 1000) if duration_s else None
            pos_y = int(title_pos_y) if title_pos_y is not None else 10
            pos_tag = r"{\pos(540," + str(pos_y) + r")}"
            ev_text = pos_tag + pysubs2.make_esc(title_text)
            if end_ms:
                ev = pysubs2.SSAEvent(start=0, end=end_ms, text=ev_text, style="Title")
            else:
                ev = pysubs2.SSAEvent(start=0, end=24 * 3600 * 1000, text=ev_text, style="Title")
            subs.events.insert(0, ev)

        subs.save(str(ass_path))
        return True, str(ass_path)
    except Exception as e:
        return False, f"ASS generation error: {e}"