#!/usr/bin/env python3
from __future__ import annotations

import sys
import os
import json
import argparse
import concurrent.futures
from pathlib import Path

# Ensure local module resolution
THIS_DIR = os.path.dirname(os.path.abspath(__file__))
if THIS_DIR not in sys.path:
    sys.path.insert(0, THIS_DIR)

from media_utils import find_ffprobe, find_ffmpeg, nvenc_available  # type: ignore
from clip_edit_process import edit_clip  # type: ignore

def main():
    parser = argparse.ArgumentParser(description="Edit downloaded clips; fast rounded frame + optional logo; NVENC supported.")
    parser.add_argument("--min-duration", type=int, default=8)
    parser.add_argument("--max-concurrent", type=int, default=3)
    parser.add_argument("--max-total", type=int, default=1000)
    parser.add_argument("--downloads-dir", default="downloads")
    parser.add_argument("--outputs-dir", default="outputs")
    parser.add_argument("--ffprobe-path", default=None)
    parser.add_argument("--ffmpeg-path", default=None)
    parser.add_argument("--watermark", default=None)
    parser.add_argument("--produce-feed", action="store_true")
    parser.add_argument("--produce-reels", action="store_true")
    parser.add_argument("--subtitle-size", type=int, default=6)
    parser.add_argument("--subtitle-margin", type=int, default=100)
    parser.add_argument("--side-margin", type=int, default=100)
    parser.add_argument("--title-enable", type=lambda v: v.lower() not in ("0","false","no"), default=True)
    parser.add_argument("--title-text", default=None)
    parser.add_argument("--title-size", type=int, default=50)
    parser.add_argument("--title-margin-top", type=int, default=80)
    parser.add_argument("--font-file", default="fonts/roboto.ttf")
    parser.add_argument("--font-family", default="Roboto")
    parser.add_argument("--corner-radius", type=int, default=60)

    # GPU
    parser.add_argument("--encoder", choices=["auto", "h264_nvenc", "libx264"], default="auto")
    parser.add_argument("--nvenc-preset", default="p5")
    parser.add_argument("--nvenc-cq", type=int, default=23)
    parser.add_argument("--nvenc-gpu", type=int, default=None)

    # Logo
    parser.add_argument("--logo-path", default=None, help="Optional PNG/SVG/JPG logo path (will be rasterized if needed). If omitted, dummy logo is generated.")
    parser.add_argument("--logo-size-ratio", type=float, default=0.15, help="Logo width ratio relative to video content width (0..1)")
    parser.add_argument("--logo-margin", type=int, default=16, help="Margin in px from video content's top-left")
    parser.add_argument("--logo-opacity", type=int, default=200, help="Logo opacity 0..255")

    args = parser.parse_args()

    ffprobe_cmd = find_ffprobe(args.ffprobe_path)
    if not ffprobe_cmd:
        print("ERROR: ffprobe not found. Use --ffprobe-path or set FFPROBE_PATH or add ffprobe to PATH.")
        sys.exit(1)
    ffmpeg_cmd = find_ffmpeg(args.ffmpeg_path)
    if not ffmpeg_cmd:
        print("ERROR: ffmpeg not found. Use --ffmpeg-path or set FFMPEG_PATH or add ffmpeg to PATH.")
        sys.exit(1)

    nvenc_ok = nvenc_available(ffmpeg_cmd)
    if args.encoder == "h264_nvenc":
        video_encoder = "h264_nvenc" if nvenc_ok else "libx264"
        if not nvenc_ok:
            print("Warning: h264_nvenc not available in this ffmpeg build. Falling back to libx264.")
    elif args.encoder == "auto":
        video_encoder = "h264_nvenc" if nvenc_ok else "libx264"
        print(f"Encoder selected: {video_encoder} (NVENC available: {nvenc_ok})")
    else:
        video_encoder = "libx264"

    downloads_dir = Path(args.downloads_dir)
    outputs_dir = Path(args.outputs_dir)
    downloads_dir.mkdir(parents=True, exist_ok=True)
    outputs_dir.mkdir(parents=True, exist_ok=True)

    watermark_path = Path(args.watermark) if args.watermark else None

    if args.font_file:
        pf = Path(args.font_file)
        if not pf.exists():
            print(f"Warning: font file '{args.font_file}' not found. Falling back to system fonts.")

    print("Using ffprobe:", ffprobe_cmd)
    print("Using ffmpeg:", ffmpeg_cmd)

    pairs = []
    for mp4 in sorted(downloads_dir.glob("*.mp4")):
        meta = mp4.with_suffix(".json")
        if meta.exists():
            pairs.append((mp4, meta))
        if len(pairs) >= args.max_total:
            break

    if not pairs:
        print("No downloaded clips with matching metadata JSON found in downloads directory.")
        sys.exit(0)

    print(f"Found {len(pairs)} downloaded clip(s). Starting processing with concurrency={args.max_concurrent}")

    results = []

    def worker(mp4_path: Path, meta_path: Path):
        try:
            src = json.loads(meta_path.read_text(encoding="utf-8"))
            if not isinstance(src, dict):
                src = {}
        except Exception:
            src = {}
        return edit_clip(
            mp4_path, src, args.min_duration, downloads_dir, outputs_dir,
            ffprobe_cmd, ffmpeg_cmd,
            args.subtitle_size, args.subtitle_margin, args.side_margin,
            args.produce_feed, args.produce_reels, watermark_path,
            args.title_enable, args.title_text, args.title_size, args.title_margin_top,
            args.font_file, args.font_family, args.corner_radius,
            # GPU
            video_encoder=video_encoder,
            nvenc_preset=args.nvenc_preset,
            nvenc_cq=args.nvenc_cq,
            nvenc_gpu=args.nvenc_gpu,
            # Logo
            logo_path=args.logo_path,
            logo_size_ratio=args.logo_size_ratio,
            logo_margin=args.logo_margin,
            logo_opacity=args.logo_opacity,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_concurrent) as exe:
        futures = [exe.submit(worker, mp4, meta) for mp4, meta in pairs]
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