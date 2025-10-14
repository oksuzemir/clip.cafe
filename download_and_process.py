#!/usr/bin/env python3
"""
download_and_process.py

Single-file pipeline producing Instagram-ready vertical posts with bottom-centered small subtitles,
and an automatic film title rendered just above the video area (not at the absolute top).

Behavior change (kept original CLI variable names):
- The script now computes the actual video area (after scaling to target width) and places the title
  a fixed gap above that video area. This uses the existing --title-margin-top value as the gap (pixels)
  between the top of the video content and the title text.
- If pysubs2 is available the title is embedded into the ASS file using an explicit \pos(...) override
  so positioning is precise. If pysubs2 is missing the drawtext fallback uses a computed y coordinate.
- No CLI variable names changed; semantics of --title-margin-top are now: gap (px) above the video area.

Additional changes:
- Defaults the font file to "fonts/roboto.ttf". If provided and present, the TTF will be copied into each
  clip's outputs/<slug>/fonts/ directory and used via subtitles:fontsdir and drawtext:fontfile.
- New feature: rounded corners for the central video rectangle. You can set --corner-radius (px).
  The script will generate a per-clip mask PNG (1080x1920) with a white rounded rectangle at the
  computed video area and use ffmpeg's alphamerge+overlay to produce rounded corners.
  Pillow (PIL) is used to create the mask. If Pillow is not available, the script falls back to
  square corners.
"""
from __future__ import annotations

import os
import sys
import json
import argparse
import requests
from pathlib import Path
import subprocess
import shutil
import concurrent.futures
import time
from decimal import Decimal, ROUND_HALF_UP
from typing import Optional, Tuple

# optional dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# optional Pillow for mask generation (rounded corners)
try:
    from PIL import Image, ImageDraw
    PIL_AVAILABLE = True
except Exception:
    PIL_AVAILABLE = False

API_KEY = os.getenv("CLIPC_CAFE_API_KEY") or os.getenv("API_KEY")
if not API_KEY:
    print("Error: CLIPC_CAFE_API_KEY not set. Export it or put in .env.")
    sys.exit(1)

BASE_URL = "https://api.clip.cafe/"

# ---------- utilities ----------
def find_executable(cli_arg_path: Optional[str], env_var: Optional[str], exe_name: str) -> Optional[str]:
    if cli_arg_path:
        p = Path(cli_arg_path)
        if p.exists():
            return str(p)
        if p.is_dir():
            candidate = p / f"{exe_name}.exe"
            if candidate.exists():
                return str(candidate)
    if env_var:
        env_path = os.environ.get(env_var)
        if env_path:
            p = Path(env_path)
            if p.exists():
                return str(p)
            candidate = Path(env_path) / f"{exe_name}.exe"
            if candidate.exists():
                return str(candidate)
    which = shutil.which(exe_name)
    if which:
        return which
    if os.name == "nt":
        userprofile = os.environ.get("USERPROFILE")
        if userprofile:
            candidate = Path(userprofile) / "AppData" / "Local" / "Microsoft" / "WinGet" / "Links" / f"{exe_name}.exe"
            if candidate.exists():
                return str(candidate)
    return None

def find_ffprobe(cli_arg: Optional[str]) -> Optional[str]:
    return find_executable(cli_arg, "FFPROBE_PATH", "ffprobe")

def find_ffmpeg(cli_arg: Optional[str]) -> Optional[str]:
    return find_executable(cli_arg, "FFMPEG_PATH", "ffmpeg")

def ffprobe_duration(path: Path, ffprobe_cmd: str) -> Optional[float]:
    try:
        cmd = [ffprobe_cmd, "-v", "error", "-show_entries", "format=duration",
               "-of", "default=noprint_wrappers=1:nokey=1", str(path)]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        out = res.stdout.strip()
        return float(out) if out else None
    except Exception:
        return None

def ffprobe_video_size(path: Path, ffprobe_cmd: str) -> Optional[Tuple[int,int]]:
    """
    Return (width, height) of first video stream using ffprobe, or None on failure.
    """
    try:
        cmd = [ffprobe_cmd, "-v", "error", "-select_streams", "v:0",
               "-show_entries", "stream=width,height", "-of", "json", str(path)]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        j = json.loads(res.stdout)
        streams = j.get("streams") or []
        if not streams:
            return None
        s = streams[0]
        w = int(s.get("width"))
        h = int(s.get("height"))
        return (w, h)
    except Exception:
        return None

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

def ffmpeg_escape_text(txt: str) -> str:
    # Minimal escaping for ffmpeg drawtext usage in a single-quoted string.
    if not txt:
        return ""
    return txt.replace("\\", "\\\\").replace("'", "\\'").replace(":", "\\:").replace("%", "%%")

def ffmpeg_escape_fontfile(path: str) -> str:
    # Escape backslashes and single-quotes for use inside single-quoted drawtext fontfile=''
    return path.replace("\\", "\\\\").replace("'", "\\'")

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

def safe_filename_from_slug(slug: str) -> str:
    name = slug.replace("/", "_").replace(" ", "_")
    return "".join(c for c in name if c.isalnum() or c in "-_.").strip("-_") or f"clip_{int(time.time())}"

# ---------- SRT -> ASS conversion (pysubs2) with Title support ----------
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
            # set margins as reasonable defaults (not used when override present)
            title_style.marginv = 0
            title_style.marginl = marginl
            title_style.marginr = marginr
            title_style.outline = 1
            title_style.shadow = 0
            subs.styles["Title"] = title_style

            # create an event that spans the whole clip duration using \pos override for exact y
            end_ms = int(duration_s * 1000) if duration_s else None
            pos_y = int(title_pos_y) if title_pos_y is not None else 10
            # center x = 540 for 1080 width
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

# ---------- ffmpeg helpers ----------
def run_ffmpeg(cmd_args: list[str]) -> Tuple[bool, Optional[Exception]]:
    try:
        subprocess.run(cmd_args, check=True)
        return True, None
    except subprocess.CalledProcessError as e:
        return False, e
    except Exception as e:
        return False, e

# ---------- clip.cafe search ----------
def do_search(actor: str, min_duration: int, size: int, offset: int = 0):
    params = {
        "api_key": API_KEY,
        "actors": actor,
        "duration": f"{min_duration}-",
        "size": size,
        "from": offset
    }
    try:
        r = requests.get(BASE_URL, params=params, timeout=30)
    except Exception as e:
        return None, f"Network/search error: {e}"
    if r.status_code >= 400:
        return None, r.text
    try:
        return r.json(), None
    except Exception as e:
        return None, f"Invalid JSON response: {e}"

# ---------- Rounded-corner mask generation ----------
def generate_rounded_mask(mask_path: Path, canvas_w: int, canvas_h: int,
                          rect_x0: int, rect_y0: int, rect_x1: int, rect_y1: int,
                          radius: int) -> bool:
    """
    Create a grayscale mask PNG (L mode) with white (255) rounded rectangle where video should be opaque,
    and black (0) elsewhere. Returns True on success.
    """
    if not PIL_AVAILABLE:
        return False
    try:
        img = Image.new("L", (canvas_w, canvas_h), 0)
        draw = ImageDraw.Draw(img)
        # Pillow's rounded_rectangle draws inclusive coordinates, so pass a bbox tuple.
        draw.rounded_rectangle([rect_x0, rect_y0, rect_x1, rect_y1], radius=radius, fill=255)
        img.save(str(mask_path))
        return True
    except Exception:
        return False


def download_stream(dl_url: str, out_path: Path, timeout: int = 120) -> Tuple[bool, Optional[str]]:
    tmp_file = out_path.with_suffix(".part")
    try:
        with requests.get(dl_url, stream=True, timeout=timeout) as r:
            r.raise_for_status()
            with open(tmp_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        shutil.move(str(tmp_file), str(out_path))
        return True, None
    except Exception as e:
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except:
            pass
        return False, str(e)

# ---------- download and process for a single hit ----------
def process_hit(src: dict, min_duration: int, downloads_dir: Path, outputs_dir: Path,
                ffprobe_cmd: str, ffmpeg_cmd: str,
                subtitle_size: int, subtitle_margin: int, side_margin: int,
                produce_feed: bool, produce_reels: bool, watermark: Optional[Path],
                title_enable: bool = True, title_text_override: Optional[str] = None,
                title_size: Optional[int] = None, title_margin_top: int = 80,
                font_file: Optional[str] = None, font_family: str = "Roboto",
                corner_radius: int = 0) -> Tuple[bool, dict]:
    slug_raw = src.get("slug") or src.get("title") or f"clip_{int(time.time())}"
    slug = safe_filename_from_slug(slug_raw)
    out_mp4 = downloads_dir / f"{slug}.mp4"
    meta_file = downloads_dir / f"{slug}.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(src, f, ensure_ascii=False, indent=2)

    dl = src.get("download")
    if not dl:
        return False, {"error": "no download url", "slug": slug}

    ok, err = download_stream(dl, out_mp4)
    if not ok:
        return False, {"error": f"download failed: {err}", "slug": slug}

    # validate duration
    dur = ffprobe_duration(out_mp4, ffprobe_cmd)
    if dur is None:
        try:
            out_mp4.unlink()
        except:
            pass
        return False, {"error": "ffprobe missing or failed", "slug": slug}
    if dur < min_duration:
        try:
            out_mp4.unlink()
        except:
            pass
        return False, {"error": f"duration_too_short ({dur})", "slug": slug, "duration": dur}

    # determine title text
    title_text = None
    if title_enable:
        title_text = title_text_override or src.get("movie_title") or src.get("movie") or src.get("title") or src.get("slug")
        if isinstance(title_text, str):
            title_text = title_text.strip()
            if title_text == "":
                title_text = None

    # parse subtitles and write SRT
    subs_field = src.get("subtitles") or src.get("captions") or ""
    subs = parse_subs_field(subs_field) if subs_field else []
    srt_path = downloads_dir / f"{slug}.srt"
    ass_path = downloads_dir / f"{slug}.ass"
    if subs:
        write_srt(subs, srt_path)
    else:
        srt_path = None

    # prepare outputs dir for this clip
    clip_out = outputs_dir / slug
    clip_out.mkdir(parents=True, exist_ok=True)
    insta_post = clip_out / f"{slug}_instagram_1080x1920.mp4"
    feed_out = clip_out / f"{slug}_feed_1080x1080.mp4" if produce_feed else None
    reels_out = clip_out / f"{slug}_reels_1080x1920.mp4" if produce_reels else None

    # Copy font file into clip-local fonts dir (if provided) so libass/ffmpeg can find it via fontsdir
    fonts_dir = None
    if font_file:
        pf = Path(font_file)
        if pf.exists():
            fonts_dir = clip_out / "fonts"
            fonts_dir.mkdir(parents=True, exist_ok=True)
            try:
                shutil.copy(str(pf), str(fonts_dir / pf.name))
            except Exception:
                fonts_dir = None
        else:
            fonts_dir = None

    # IMPORTANT: compute target width and detect actual scaled video height so we can place title above video area
    side_margin = max(0, int(side_margin))
    target_width = max(16, 1080 - 2 * side_margin)

    # attempt to read original video size to calculate scaled height
    orig_size = ffprobe_video_size(out_mp4, ffprobe_cmd)
    if orig_size:
        orig_w, orig_h = orig_size
        # scaled height after scale=target_width:-2
        try:
            scaled_h = int(round(orig_h * (target_width / orig_w)))
        except Exception:
            scaled_h = None
    else:
        orig_w = orig_h = None
        scaled_h = None

    # top offset of the video area inside 1080x1920 canvas: (1920 - scaled_h) // 2
    if scaled_h and scaled_h > 0:
        video_top = (1920 - scaled_h) // 2
    else:
        # fallback: approximate center (if we couldn't determine sizes)
        video_top = 200  # a conservative default; ASS will still work but positioning may need tweak

    # We want the title to sit just above the video area.
    # The CLI param title_margin_top is used as the gap in pixels between the top of video area and the title baseline.
    # So compute absolute title y coordinate (ASS \pos uses absolute coords from top-left).
    gap = int(title_margin_top) if title_margin_top is not None else 80
    # place title baseline slightly above the video_top by gap (ensure not negative)
    title_pos_y = max(6, video_top - gap)

    # Now we attempt to generate ASS (with embedded title positioned at title_pos_y).
    use_ass = False
    if srt_path:
        ass_ok, ass_info = generate_ass_from_srt(
            srt_path, ass_path, font=font_family,
            size=subtitle_size, marginv=subtitle_margin, marginl=side_margin, marginr=side_margin,
            title_text=title_text if title_text else None,
            title_size=title_size if title_size else subtitle_size,
            title_pos_y=title_pos_y,
            duration_s=dur
        )
        if ass_ok:
            use_ass = True
        else:
            ass_path = None

    # Build the base transform (scale->pad->crop) as a filter snippet for filter_complex usage
    base_transform_snippet = (
        f"scale={target_width}:-2,"
        "pad=width='max(1080,iw)':height='max(1920,ih)':x='(ow-iw)/2':y='(oh-ih)/2':color=black,"
        "crop=1080:1920"
    )

    # Prepare drawtext fallback (if ASS not used)
    drawtext_filter = None
    if (not use_ass) and title_text:
        txt = ffmpeg_escape_text(title_text)
        tsize = title_size if title_size else subtitle_size
        y_pos = title_pos_y
        if font_file and Path(font_file).exists():
            ff_fontfile = ffmpeg_escape_fontfile(Path(font_file).as_posix())
            drawtext_filter = f"drawtext=fontfile='{ff_fontfile}':text='{txt}':fontsize={tsize}:fontcolor=white:x=(w-text_w)/2:y={y_pos}:box=1:boxcolor=black@0.5:boxborderw=5"
        else:
            drawtext_filter = f"drawtext=font='Roboto':text='{txt}':fontsize={tsize}:fontcolor=white:x=(w-text_w)/2:y={y_pos}:box=1:boxcolor=black@0.5:boxborderw=5"

    # Helper to build the video filter chain (applied to [0:v]) before masking/compositing
    def build_content_filter_chain(base_snippet: str, use_ass_local: bool, ass_path_local: Optional[Path],
                                   srt_path_local: Optional[Path], drawtext_local: Optional[str],
                                   fonts_dir_local: Optional[Path], font_family_local: str) -> str:
        # Returns a filter chain string (no input/output labels) to apply to [0:v]
        chain = base_snippet
        # Append drawtext before subtitles/ass so title is part of the content that's masked
        if drawtext_local:
            chain += "," + drawtext_local
        if use_ass_local and ass_path_local and ass_path_local.exists():
            # Use subtitles filter (so fontsdir can be passed)
            if fonts_dir_local:
                chain += f",subtitles={ass_path_local.as_posix()}:fontsdir={fonts_dir_local.as_posix()}"
            else:
                chain += f",ass={ass_path_local.as_posix()}"
        else:
            if srt_path_local and srt_path_local.exists():
                force_style = f"FontName={font_family_local},FontSize={subtitle_size},Alignment=2,MarginV={subtitle_margin},PrimaryColour=&H00FFFFFF&,Outline=1,Shadow=0"
                if fonts_dir_local:
                    chain += f",subtitles={srt_path_local.as_posix()}:force_style='{force_style}':fontsdir={fonts_dir_local.as_posix()}"
                else:
                    chain += f",subtitles={srt_path_local.as_posix()}:force_style='{force_style}'"
        return chain

    # If corner_radius > 0 and PIL available and scaled dimensions are known, generate mask
    use_mask = False
    mask_path = clip_out / "mask_1080x1920.png"
    if corner_radius and corner_radius > 0 and PIL_AVAILABLE and scaled_h and scaled_h > 0:
        # compute video rectangle coordinates inside 1080x1920 canvas
        video_left = (1080 - target_width) // 2
        video_top = video_top  # computed above
        x0 = video_left
        y0 = video_top
        x1 = video_left + target_width
        y1 = video_top + scaled_h
        # clamp coordinates to canvas
        x0 = max(0, min(1079, int(x0)))
        y0 = max(0, min(1919, int(y0)))
        x1 = max(0, min(1080, int(x1)))
        y1 = max(0, min(1920, int(y1)))
        # radius cannot exceed half of min(width,height)
        max_radius = min((x1 - x0) // 2, (y1 - y0) // 2)
        radius_use = max(0, min(int(corner_radius), max_radius)) if max_radius > 0 else 0
        if radius_use > 0:
            if generate_rounded_mask(mask_path, 1080, 1920, x0, y0, x1, y1, radius_use):
                use_mask = True

    # Build final ffmpeg invocation for instagram vertical (with mask if available)
    content_chain = build_content_filter_chain(base_transform_snippet, use_ass, ass_path, srt_path, drawtext_filter, fonts_dir, font_family)

    if use_mask and mask_path.exists():
        # We'll use filter_complex with two inputs: video and mask
        # Steps:
        #  - [0:v] apply content_chain -> [vsub]
        #  - [1:v] scale to 1080x1920 -> [mask]
        #  - [vsub][mask] alphamerge -> [va]
        #  - color black background -> [bg]
        #  - [bg][va] overlay -> [final]
        filter_complex = (
            f"[0:v]{content_chain}[vsub];"
            f"[1:v]scale=1080:1920[mask];"
            f"[vsub][mask]alphamerge[va];"
            f"color=c=black:s=1080x1920[bg];"
            f"[bg][va]overlay=format=auto[final]"
        )
        cmd_insta = [
            ffmpeg_cmd, "-y",
            "-i", str(out_mp4),
            "-i", str(mask_path),
            "-filter_complex", filter_complex,
            "-map", "[final]",
            "-map", "0:a?",
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            str(insta_post)
        ]
    else:
        # fallback to simple -vf chain (no mask)
        vf_final = content_chain
        cmd_insta = [
            ffmpeg_cmd, "-y",
            "-i", str(out_mp4),
            "-vf", vf_final,
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-c:a", "aac", "-b:a", "128k",
            str(insta_post)
        ]

    ok, err = run_ffmpeg(cmd_insta)
    if not ok:
        return False, {"error": f"ffmpeg instagram_post failed: {err}", "slug": slug}

    # 2) optional feed (1080x1080) - NOTE: mask currently applied only to instagram/reels (1080x1920).
    if produce_feed:
        # reuse previous behavior for feed (no mask). Could be extended to generate a feed-specific mask similarly.
        vf_feed = (
            "scale=1080:-2,"
            "pad=width='max(1080,iw)':height='max(1080,ih)':x='(ow-iw)/2':y='(oh-ih)/2':color=black,"
            "crop=1080:1080"
        )
        # build simple chain for feed with drawtext/subtitles/ass
        feed_content_chain = build_content_filter_chain(vf_feed, use_ass, ass_path, srt_path, drawtext_filter, fonts_dir, font_family)
        cmd_feed = [
            ffmpeg_cmd, "-y",
            "-i", str(out_mp4),
            "-vf", feed_content_chain,
            "-c:v", "libx264", "-preset", "fast", "-crf", "22",
            "-c:a", "aac", "-b:a", "128k",
            str(feed_out)
        ]
        ok, err = run_ffmpeg(cmd_feed)
        if not ok:
            return False, {"error": f"ffmpeg feed failed: {err}", "slug": slug}

    # 3) optional reels (same as insta_post)
    if produce_reels:
        if use_mask and mask_path.exists():
            # reuse same filter_complex approach as insta_post (mask already scaled for 1080x1920)
            filter_complex = (
                f"[0:v]{content_chain}[vsub];"
                f"[1:v]scale=1080:1920[mask];"
                f"[vsub][mask]alphamerge[va];"
                f"color=c=black:s=1080x1920[bg];"
                f"[bg][va]overlay=format=auto[final]"
            )
            cmd_reels = [
                ffmpeg_cmd, "-y",
                "-i", str(out_mp4),
                "-i", str(mask_path),
                "-filter_complex", filter_complex,
                "-map", "[final]",
                "-map", "0:a?",
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                str(reels_out)
            ]
        else:
            vf_reels_final = content_chain
            cmd_reels = [
                ffmpeg_cmd, "-y",
                "-i", str(out_mp4),
                "-vf", vf_reels_final,
                "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                "-c:a", "aac", "-b:a", "128k",
                str(reels_out)
            ]

        ok, err = run_ffmpeg(cmd_reels)
        if not ok:
            return False, {"error": f"ffmpeg reels failed: {err}", "slug": slug}
        if watermark:
            if not watermark.exists():
                return False, {"error": f"watermark not found: {watermark}", "slug": slug}
            reels_wm = clip_out / f"{slug}_reels_wm.mp4"
            cmd_wm = [ffmpeg_cmd, "-y", "-i", str(reels_out), "-i", str(watermark),
                      "-filter_complex", "overlay=10:10", "-c:v", "libx264", "-preset", "fast", "-crf", "23",
                      "-c:a", "aac", "-b:a", "128k", str(reels_wm)]
            ok, err = run_ffmpeg(cmd_wm)
            if not ok:
                return False, {"error": f"ffmpeg watermark failed: {err}", "slug": slug}

    return True, {
        "slug": slug,
        "duration": dur,
        "outputs": {
            "instagram_post": str(insta_post),
            "feed": str(feed_out) if produce_feed else None,
            "reels": str(reels_out) if produce_reels else None
        },
        "meta": str(meta_file)
    }

# ---------- CLI and orchestration ----------
def main():
    parser = argparse.ArgumentParser(description="Download clips and produce Instagram-ready vertical posts with bottom-centered subtitles and top title.")
    parser.add_argument("--actor", default="Tom Hardy", help="Actor name to search")
    parser.add_argument("--min-duration", type=int, default=8, help="Minimum clip duration (inclusive)")
    parser.add_argument("--size", type=int, default=100, help="Search size (how many hits to request)")
    parser.add_argument("--offset", type=int, default=0, help="Search offset (pagination)")
    parser.add_argument("--max-concurrent", type=int, default=3, help="Max concurrent downloads")
    parser.add_argument("--max-total", type=int, default=3, help="Max total clips to download/process")
    parser.add_argument("--downloads-dir", default="downloads", help="Downloads directory")
    parser.add_argument("--outputs-dir", default="outputs", help="Outputs directory")
    parser.add_argument("--ffprobe-path", default=None, help="Explicit ffprobe path")
    parser.add_argument("--ffmpeg-path", default=None, help="Explicit ffmpeg path")
    parser.add_argument("--watermark", default=None, help="Optional watermark PNG (applies to reels when produced)")
    parser.add_argument("--produce-feed", action="store_true", help="Also produce feed (1080x1080)")
    parser.add_argument("--produce-reels", action="store_true", help="Also produce reels (1080x1920)")
    parser.add_argument("--subtitle-size", type=int, default=10, help="Subtitle font size (ASS)")
    parser.add_argument("--subtitle-margin", type=int, default=40, help="Subtitle vertical margin (MarginV in ASS). Reduce if subs are too high.")
    parser.add_argument("--side-margin", type=int, default=20, help="Left/right margin inside 1080x1920 canvas (px)")
    parser.add_argument("--title-enable", type=lambda v: v.lower() not in ("0","false","no"), default=True, help="Enable title rendering at top (true/false)")
    parser.add_argument("--title-text", default=None, help="Override title text (otherwise uses metadata movie_title/title/slug)")
    parser.add_argument("--title-size", type=int, default=44, help="Title font size (defaults to subtitle-size if not set)")
    parser.add_argument("--title-margin-top", type=int, default=80, help="Gap in pixels between top of video area and the title")
    parser.add_argument("--font-file", default="fonts/roboto.ttf", help="Path to Roboto (TTF) to use directly (default: fonts/roboto.ttf)")
    parser.add_argument("--font-family", default="Roboto", help="Font family name to use in ASS/subtitles (default: Roboto)")
    parser.add_argument("--corner-radius", type=int, default=0, help="Corner radius in pixels for the central video rectangle (0 = no rounding)")
    args = parser.parse_args()

    ffprobe_cmd = find_ffprobe(args.ffprobe_path)
    if not ffprobe_cmd:
        print("ERROR: ffprobe not found. Use --ffprobe-path or set FFPROBE_PATH or add ffprobe to PATH.")
        sys.exit(1)
    ffmpeg_cmd = find_ffmpeg(args.ffmpeg_path)
    if not ffmpeg_cmd:
        print("ERROR: ffmpeg not found. Use --ffmpeg-path or set FFMPEG_PATH or add ffmpeg to PATH.")
        sys.exit(1)

    downloads_dir = Path(args.downloads_dir)
    outputs_dir = Path(args.outputs_dir)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    watermark_path = Path(args.watermark) if args.watermark else None

    # Warn if default font-file not found (we'll fall back to system fonts)
    if args.font_file:
        pf = Path(args.font_file)
        if not pf.exists():
            print(f"Warning: font file '{args.font_file}' not found. The script will fall back to system fonts unless you provide a valid --font-file path.")

    if args.corner_radius and args.corner_radius > 0 and not PIL_AVAILABLE:
        print("Warning: --corner-radius requested but Pillow (PIL) not available. Install pillow (pip install pillow) to enable rounded masks. Falling back to square corners.")

    print("Using ffprobe:", ffprobe_cmd)
    print("Using ffmpeg:", ffmpeg_cmd)
    print(f"Searching for actor='{args.actor}' duration>={args.min_duration}s size={args.size} offset={args.offset}")

    js, err = do_search(args.actor, args.min_duration, args.size, offset=args.offset)
    if err:
        print("Search error:", err[:2000])
        sys.exit(1)
    hits = js.get("hits", {}).get("hits", [])
    if not hits:
        print("No hits returned.")
        sys.exit(0)

    # collect candidate hits (metadata duration filter and presence of download URL)
    candidates = []
    for h in hits:
        src = h.get("_source", {}) or {}
        meta_dur = src.get("duration")
        try:
            meta_dur_int = int(meta_dur) if meta_dur is not None else None
        except:
            meta_dur_int = None
        if meta_dur_int is not None and meta_dur_int < args.min_duration:
            continue
        if not src.get("download"):
            continue
        candidates.append(src)
        if len(candidates) >= args.max_total:
            break

    if not candidates:
        print("No candidate clips found meeting metadata>=min-duration and having download URL.")
        sys.exit(0)

    print(f"Found {len(candidates)} candidate(s). Starting download+process with concurrency={args.max_concurrent}")

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_concurrent) as exe:
        futures = []
        for src in candidates:
            futures.append(exe.submit(
                process_hit, src, args.min_duration, downloads_dir, outputs_dir,
                ffprobe_cmd, ffmpeg_cmd,
                args.subtitle_size, args.subtitle_margin, args.side_margin,
                args.produce_feed, args.produce_reels, watermark_path,
                args.title_enable, args.title_text, args.title_size, args.title_margin_top,
                args.font_file, args.font_family, args.corner_radius
            ))
        for fut in concurrent.futures.as_completed(futures):
            try:
                ok, info = fut.result()
                results.append((ok, info))
                if ok:
                    print("OK ->", json.dumps(info, ensure_ascii=False, indent=2))
                else:
                    print("FAILED ->", info)
            except Exception as e:
                print("Exception in worker:", e)

    succ = sum(1 for r in results if r[0])
    fail = len(results) - succ
    print(f"Summary: success={succ}, failed={fail}")
    print("Outputs directory:", outputs_dir)

if __name__ == "__main__":
    main()