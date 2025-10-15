from __future__ import annotations

from pathlib import Path
from typing import Tuple

# optional Pillow
try:
    from PIL import Image, ImageDraw
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False


def _parse_rgba(color_str: str, alpha: int = 255) -> Tuple[int, int, int, int]:
    """
    Parse '#RRGGBB' or named 'black'/'white' to RGBA tuple.
    """
    if not color_str:
        return (0, 0, 0, alpha)
    s = color_str.strip().lower()
    if s.startswith("#") and len(s) == 7:
        r = int(s[1:3], 16)
        g = int(s[3:5], 16)
        b = int(s[5:7], 16)
        return (r, g, b, alpha)
    if s in ("black", "bk", "0", "000000"):
        return (0, 0, 0, alpha)
    if s in ("white", "fff", "ffffff", "1"):
        return (255, 255, 255, alpha)
    # fallback
    return (0, 0, 0, alpha)


def generate_rounded_mask(mask_path: Path, canvas_w: int, canvas_h: int,
                          rect_x0: int, rect_y0: int, rect_x1: int, rect_y1: int,
                          radius: int) -> bool:
    """
    Eski yöntem (videoyu alfalı kesmek için): RGBA PNG üretip alfa kanalında içi 255, dışı 0.
    Artık hızlı yol olarak generate_rounded_frame_window tercih ediliyor.
    """
    if not PIL_AVAILABLE:
        return False
    try:
        img = Image.new("RGBA", (canvas_w, canvas_h), (0, 0, 0, 0))
        alpha = Image.new("L", (canvas_w, canvas_h), 0)
        draw = ImageDraw.Draw(alpha)
        x0, y0 = int(rect_x0), int(rect_y0)
        x1, y1 = int(rect_x1) - 1, int(rect_y1) - 1
        x0 = max(0, min(canvas_w - 1, x0))
        y0 = max(0, min(canvas_h - 1, y0))
        x1 = max(0, min(canvas_w - 1, x1))
        y1 = max(0, min(canvas_h - 1, y1))
        r = max(0, int(radius))
        draw.rounded_rectangle([x0, y0, x1, y1], radius=r, fill=255)
        img.putalpha(alpha)
        mask_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(mask_path))
        return True
    except Exception:
        return False


def generate_rounded_frame_window(frame_path: Path,
                                  canvas_w: int,
                                  canvas_h: int,
                                  rect_x0: int, rect_y0: int, rect_x1: int, rect_y1: int,
                                  radius: int,
                                  bg_color: str = "black") -> bool:
    """
    Hızlı yöntem: Üstte overlay edilecek bir RGBA 'çerçeve' üretir.
    - Tuval: BG renginde opak
    - Ortadaki yuvarlak dikdörtgen alan: tamamen şeffaf (video bu pencereden görünür)
    Bu sayede ffmpeg'de yalnızca overlay=0:0 ile çok hızlı uygulanır.
    """
    if not PIL_AVAILABLE:
        return False
    try:
        bg_rgba = _parse_rgba(bg_color, 255)
        img = Image.new("RGBA", (canvas_w, canvas_h), bg_rgba)

        # Şeffaf pencereyi oluşturmak için ayrı bir alfa maskesi hazırlayıp 0'a çekeceğiz
        alpha = Image.new("L", (canvas_w, canvas_h), 255)  # tümü opak
        draw = ImageDraw.Draw(alpha)

        # Yuvarlak pencereyi 0 (şeffaf) olarak doldur
        x0, y0 = int(rect_x0), int(rect_y0)
        x1, y1 = int(rect_x1) - 1, int(rect_y1) - 1
        x0 = max(0, min(canvas_w - 1, x0))
        y0 = max(0, min(canvas_h - 1, y0))
        x1 = max(0, min(canvas_w - 1, x1))
        y1 = max(0, min(canvas_h - 1, y1))
        r = max(0, int(radius))
        # önce opak bir arkaplan; sonra pencereyi 0 ile "oyuyoruz"
        # Bunun için önce hiçbir şey çizmeyip sadece pencereyi 0 ile dolduralım:
        # Pillow '0' doldurmak için şeklin etrafını 255’de bırakır.
        draw.rounded_rectangle([x0, y0, x1, y1], radius=r, fill=0)

        # alfayı uygula
        img.putalpha(alpha)
        frame_path.parent.mkdir(parents=True, exist_ok=True)
        img.save(str(frame_path))
        return True
    except Exception:
        return False