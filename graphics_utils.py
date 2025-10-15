from __future__ import annotations

from pathlib import Path
from typing import Optional, Tuple

# optional Pillow
try:
    from PIL import Image, ImageDraw, ImageFont
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False


def _apply_opacity(img: "Image.Image", alpha_0_255: int) -> "Image.Image":
    alpha_0_255 = max(0, min(255, int(alpha_0_255)))
    if img.mode != "RGBA":
        img = img.convert("RGBA")
    r, g, b, a = img.split()
    # combine existing alpha with target alpha
    a = a.point(lambda x: int(x * (alpha_0_255 / 255.0)))
    img.putalpha(a)
    return img


def _scale_width_keep_ar(img: "Image.Image", target_w: int) -> "Image.Image":
    target_w = max(1, int(target_w))
    w, h = img.size
    if w == target_w:
        return img
    new_h = max(1, int(round(h * (target_w / float(w)))))
    return img.resize((target_w, new_h), Image.LANCZOS)


def _make_dummy_logo(base_w: int = 512, base_h: int = 256, alpha_0_255: int = 200) -> "Image.Image":
    # Semi-transparent white rounded rectangle with "LOGO" text
    img = Image.new("RGBA", (base_w, base_h), (255, 255, 255, 0))
    draw = ImageDraw.Draw(img)
    radius = int(min(base_w, base_h) * 0.18)
    # background rect
    try:
        draw.rounded_rectangle([0, 0, base_w - 1, base_h - 1], radius=radius, fill=(255, 255, 255, alpha_0_255))
    except Exception:
        # fallback if rounded_rectangle isn't available
        draw.rectangle([0, 0, base_w - 1, base_h - 1], fill=(255, 255, 255, alpha_0_255))
    # text
    try:
        font = ImageFont.load_default()
        text = "LOGO"
        tw, th = draw.textbbox((0, 0), text, font=font)[2:]
        draw.text(((base_w - tw) // 2, (base_h - th) // 2), text, fill=(0, 0, 0, 200), font=font)
    except Exception:
        pass
    return img


def prepare_logo(
    out_path: Path,
    *,
    logo_src: Optional[Path],
    desired_width: int,
    opacity_0_255: int = 200,
) -> Optional[Path]:
    """
    Produce an RGBA logo PNG at out_path:
      - If logo_src is provided and exists, load it, convert to RGBA, apply opacity, scale to desired_width.
      - Else generate a dummy logo, then scale.
    Returns out_path or None on failure.
    """
    if not PIL_AVAILABLE:
        return None
    try:
        if logo_src and logo_src.exists():
            img = Image.open(str(logo_src)).convert("RGBA")
        else:
            img = _make_dummy_logo(alpha_0_255=opacity_0_255)
        img = _apply_opacity(img, opacity_0_255)
        img = _scale_width_keep_ar(img, max(1, int(desired_width)))
        out_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(out_path))
        return out_path
    except Exception:
        return None