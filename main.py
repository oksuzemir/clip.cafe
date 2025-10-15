#!/usr/bin/env python3
from __future__ import annotations

import sys
import json
import time
import argparse
import concurrent.futures
from pathlib import Path
from typing import Optional

from clipcafe_client import do_search, download_stream, get_api_key
from media_utils import find_ffprobe, find_ffmpeg, ffprobe_duration
from clip_edit_process import edit_clip, safe_filename_from_slug

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

    # API key behavior (same as original): required for main (search).
    if not get_api_key():
        print("Error: CLIPC_CAFE_API_KEY not set. Export it or put in .env.")
        sys.exit(1)

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

    if args.font_file:
        pf = Path(args.font_file)
        if not pf.exists():
            print(f"Warning: font file '{args.font_file}' not found. The script will fall back to system fonts unless you provide a valid --font-file path.")

    if args.corner_radius and args.corner_radius > 0:
        try:
            from mask_utils import PIL_AVAILABLE
            if not PIL_AVAILABLE:
                print("Warning: --corner-radius requested but Pillow (PIL) not available. Install pillow (pip install pillow) to enable rounded masks. Falling back to square corners.")
        except Exception:
            pass

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
    def worker(src: dict):
        slug_raw = src.get("slug") or src.get("title") or f"clip_{int(time.time())}"
        slug = safe_filename_from_slug(slug_raw)
        out_mp4 = downloads_dir / f"{slug}.mp4"
        meta_file = downloads_dir / f"{slug}.json"
        try:
            meta_file.write_text(json.dumps(src, ensure_ascii=False, indent=2), encoding="utf-8")
        except Exception:
            pass

        dl = src.get("download")
        if not dl:
            return False, {"error": "no download url", "slug": slug}

        ok, err = download_stream(dl, out_mp4)
        if not ok:
            return False, {"error": f"download failed: {err}", "slug": slug}

        # validate duration before processing (same as original)
        dur = ffprobe_duration(out_mp4, ffprobe_cmd)
        if dur is None:
            try:
                out_mp4.unlink()
            except:
                pass
            return False, {"error": "ffprobe missing or failed", "slug": slug}
        if dur < args.min_duration:
            try:
                out_mp4.unlink()
            except:
                pass
            return False, {"error": f"duration_too_short ({dur})", "slug": slug, "duration": dur}

        return edit_clip(
            out_mp4, src, args.min_duration, downloads_dir, outputs_dir,
            ffprobe_cmd, ffmpeg_cmd,
            args.subtitle_size, args.subtitle_margin, args.side_margin,
            args.produce_feed, args.produce_reels, watermark_path,
            args.title_enable, args.title_text, args.title_size, args.title_margin_top,
            args.font_file, args.font_family, args.corner_radius
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_concurrent) as exe:
        futures = [exe.submit(worker, src) for src in candidates]
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