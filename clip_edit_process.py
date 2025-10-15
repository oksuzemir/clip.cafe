from __future__ import annotations

import os
import json
import time
import shutil
from pathlib import Path
from typing import Optional, Tuple

from media_utils import (
    ffprobe_duration,
    ffprobe_video_size,
    ffmpeg_escape_text,
    ffmpeg_escape_fontfile,
    run_ffmpeg,
)
from subs_utils import parse_subs_field, write_srt, generate_ass_from_srt
from mask_utils import (
    generate_rounded_mask,
    generate_rounded_frame_window,
    PIL_AVAILABLE,
)
from graphics_utils import prepare_logo

DEBUG = os.getenv("MASK_DEBUG", "0") in ("1", "true", "True", "YES", "yes")
BG_COLOR = os.getenv("BG_COLOR", "black")  # örn: '#0d0d0d' yaparsan köşeler daha net görünür

def log_debug(*args):
    if DEBUG:
        print("[mask-debug]", *args)

def safe_filename_from_slug(slug: str) -> str:
    name = slug.replace("/", "_").replace(" ", "_")
    return "".join(c for c in name if c.isalnum() or c in "-_.").strip("-_") or f"clip_{int(time.time())}"

def _build_vencoder_args(
    video_encoder: str,
    nvenc_preset: str,
    nvenc_cq: int,
    nvenc_gpu: Optional[int]
) -> list[str]:
    if video_encoder == "h264_nvenc":
        args = [
            "-c:v", "h264_nvenc",
            "-preset", nvenc_preset,
            "-tune", "hq",
            "-rc", "vbr",
            "-cq", str(int(nvenc_cq)),
            "-b:v", "0",
            "-profile:v", "high",
        ]
        if nvenc_gpu is not None:
            args += ["-gpu", str(int(nvenc_gpu))]
        return args
    else:
        return [
            "-c:v", "libx264",
            "-preset", "veryfast",
            "-crf", "23",
            "-profile:v", "high",
        ]

def edit_clip(
    video_path: Path,
    src: dict,
    min_duration: int,
    downloads_dir: Path,
    outputs_dir: Path,
    ffprobe_cmd: str,
    ffmpeg_cmd: str,
    subtitle_size: int,
    subtitle_margin: int,
    side_margin: int,
    produce_feed: bool,
    produce_reels: bool,
    watermark: Optional[Path],
    title_enable: bool = True,
    title_text_override: Optional[str] = None,
    title_size: Optional[int] = None,
    title_margin_top: int = 80,
    font_file: Optional[str] = None,
    font_family: str = "Roboto",
    corner_radius: int = 100,
    # GPU params
    video_encoder: str = "libx264",
    nvenc_preset: str = "p5",
    nvenc_cq: int = 23,
    nvenc_gpu: Optional[int] = None,
    # Logo params
    logo_path: Optional[str] = None,
    logo_size_ratio: float = 0.15,  # logo width as fraction of video content width
    logo_margin: int = 16,          # px from video area's top-left
    logo_opacity: int = 200         # 0-255
) -> Tuple[bool, dict]:
    slug_raw = src.get("slug") or src.get("title") or video_path.stem
    slug = safe_filename_from_slug(slug_raw)

    dur = ffprobe_duration(video_path, ffprobe_cmd)
    if dur is None:
        return False, {"error": "ffprobe missing or failed", "slug": slug}
    if dur < min_duration:
        return False, {"error": f"duration_too_short ({dur})", "slug": slug, "duration": dur}

    title_text = None
    if title_enable:
        title_text = title_text_override or src.get("movie_title") or src.get("movie") or src.get("title") or src.get("slug")
        if isinstance(title_text, str):
            title_text = title_text.strip()
            if title_text == "":
                title_text = None

    subs_field = src.get("subtitles") or src.get("captions") or ""
    subs = parse_subs_field(subs_field) if subs_field else []
    srt_path = downloads_dir / f"{slug}.srt"
    ass_path = downloads_dir / f"{slug}.ass"
    if subs:
        write_srt(subs, srt_path)
    else:
        srt_path = None

    clip_out = outputs_dir / slug
    clip_out.mkdir(parents=True, exist_ok=True)
    insta_post = clip_out / f"{slug}_instagram_1080x1920.mp4"
    feed_out = clip_out / f"{slug}_feed_1080x1080.mp4" if produce_feed else None
    reels_out = clip_out / f"{slug}_reels_1080x1920.mp4" if produce_reels else None

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

    # Layout: compute video content region inside 1080x1920
    side_margin = max(0, int(side_margin))
    target_width = max(16, 1080 - 2 * side_margin)

    orig_size = ffprobe_video_size(video_path, ffprobe_cmd)
    if orig_size:
        orig_w, orig_h = orig_size
        try:
            scaled_h = int(round(orig_h * (target_width / orig_w)))
        except Exception:
            scaled_h = None
    else:
        scaled_h = None

    if scaled_h and scaled_h > 0:
        video_top_val = (1920 - scaled_h) // 2
    else:
        video_top_val = 200  # fallback conservative

    gap = int(title_margin_top) if title_margin_top is not None else 80
    title_pos_y = max(6, video_top_val - gap)

    # ASS generation (subtitles + optional title)
    use_ass = False
    if srt_path:
        ass_ok, _ = generate_ass_from_srt(
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

    # Base video (no text)
    base_transform_snippet = (
        f"scale={target_width}:-2,"
        "pad=width='max(1080,iw)':height='max(1920,ih)':x='(ow-iw)/2':y='(oh-ih)/2':color=black,"
        "crop=1080:1920"
    )

    # Text suffix (applied at the end)
    def build_text_suffix(use_ass_local: bool,
                          ass_path_local: Optional[Path],
                          srt_path_local: Optional[Path],
                          drawtext_local: Optional[str],
                          fonts_dir_local: Optional[Path],
                          font_family_local: str,
                          subtitle_size_local: int,
                          subtitle_margin_local: int) -> str:
        parts = []
        if drawtext_local:
            parts.append(drawtext_local)
        if use_ass_local and ass_path_local and ass_path_local.exists():
            if fonts_dir_local:
                parts.append(f"subtitles={ass_path_local.as_posix()}:fontsdir={fonts_dir_local.as_posix()}")
            else:
                parts.append(f"ass={ass_path_local.as_posix()}")
        else:
            if srt_path_local and srt_path_local.exists():
                force_style = f"FontName={font_family_local},FontSize={subtitle_size_local},Alignment=2,MarginV={subtitle_margin_local},PrimaryColour=&H00FFFFFF&,Outline=1,Shadow=0"
                if fonts_dir_local:
                    parts.append(f"subtitles={srt_path_local.as_posix()}:force_style='{force_style}':fontsdir={fonts_dir_local.as_posix()}")
                else:
                    parts.append(f"subtitles={srt_path_local.as_posix()}:force_style='{force_style}'")
        return ("," + ",".join(parts)) if parts else ""

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

    text_suffix = build_text_suffix(
        use_ass, ass_path, srt_path, drawtext_filter, fonts_dir, font_family, subtitle_size, subtitle_margin
    )

    # Fast rounded frame (window) path
    use_frame = False
    frame_path = clip_out / "frame_1080x1920.png"
    if corner_radius and corner_radius > 0 and PIL_AVAILABLE and scaled_h and scaled_h > 0:
        video_left = (1080 - target_width) // 2
        x0 = max(0, min(1079, int(video_left)))
        y0 = max(0, min(1919, int(video_top_val)))
        x1 = max(1, min(1080, int(video_left + target_width)))
        y1 = max(1, min(1920, int(video_top_val + scaled_h)))
        max_radius = min((x1 - x0) // 2, (y1 - y0) // 2)
        radius_use = max(0, min(int(corner_radius), max_radius)) if max_radius > 0 else 0
        if radius_use > 0:
            ok_frame = generate_rounded_frame_window(
                frame_path, 1080, 1920, x0, y0, x1, y1, radius_use, bg_color=BG_COLOR
            )
            use_frame = bool(ok_frame and frame_path.exists())
            log_debug("frame_window", "rect", (x0, y0, x1, y1), "radius", radius_use, "ok", use_frame)
        else:
            log_debug("radius_use=0; frame skipped")

    # Fall back to (slower) alpha mask if frame not used
    use_mask = False
    mask_path = clip_out / "mask_1080x1920.png"
    if (not use_frame) and corner_radius and corner_radius > 0 and PIL_AVAILABLE and scaled_h and scaled_h > 0:
        video_left = (1080 - target_width) // 2
        x0 = video_left
        y0 = video_top_val
        x1 = video_left + target_width
        y1 = video_top_val + scaled_h
        x0 = max(0, min(1079, int(x0)))
        y0 = max(0, min(1919, int(y0)))
        x1 = max(1, min(1080, int(x1)))
        y1 = max(1, min(1920, int(y1)))
        max_radius = min((x1 - x0) // 2, (y1 - y0) // 2)
        radius_use = max(0, min(int(corner_radius), max_radius)) if max_radius > 0 else 0
        if radius_use > 0:
            if generate_rounded_mask(mask_path, 1080, 1920, x0, y0, x1, y1, radius_use):
                use_mask = True

    # Prepare logo (dummy or provided), scaled relative to content width
    logo_ready_path: Optional[Path] = None
    try:
        if logo_size_ratio and logo_size_ratio > 0:
            desired_logo_w = max(8, int(round(target_width * float(logo_size_ratio))))
            logo_src = Path(logo_path) if logo_path else None
            logo_ready_path = prepare_logo(
                clip_out / "logo_rgba.png",
                logo_src=logo_src,
                desired_width=desired_logo_w,
                opacity_0_255=int(logo_opacity),
            )
    except Exception:
        logo_ready_path = None

    # Encoder args
    venc_args = _build_vencoder_args(video_encoder, nvenc_preset, nvenc_cq, nvenc_gpu)

    # Overlay coords for logo (relative to video area)
    video_left = (1080 - target_width) // 2
    lx = int(video_left + max(0, int(logo_margin)))
    ly = int(video_top_val + max(0, int(logo_margin)))

    # 1) Instagram 1080x1920
    if use_frame and frame_path.exists():
        if logo_ready_path and logo_ready_path.exists():
            # Inputs: 0=video, 1=frame, 2=logo
            filter_complex = (
                f"[0:v]{base_transform_snippet}[vcore];"
                f"[vcore][1:v]overlay=0:0[withframe];"
                f"[withframe][2:v]overlay={lx}:{ly}"
                f"{text_suffix}"
                f"[final]"
            )
            cmd_insta = [
                ffmpeg_cmd, "-y",
                "-i", str(video_path),
                "-i", str(frame_path),
                "-i", str(logo_ready_path),
                "-filter_complex", filter_complex,
                "-map", "[final]", "-map", "0:a?",
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(insta_post)
            ]
        else:
            filter_complex = (
                f"[0:v]{base_transform_snippet}[vcore];"
                f"[vcore][1:v]overlay=0:0"
                f"{text_suffix}"
                f"[final]"
            )
            cmd_insta = [
                ffmpeg_cmd, "-y",
                "-i", str(video_path),
                "-i", str(frame_path),
                "-filter_complex", filter_complex,
                "-map", "[final]", "-map", "0:a?",
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(insta_post)
            ]
    elif use_mask and mask_path.exists():
        if logo_ready_path and logo_ready_path.exists():
            # Inputs: 0=video, 1=mask, 2=logo
            filter_complex = (
                f"[0:v]{base_transform_snippet},format=rgba[vcore];"
                f"[1:v]scale=1080:1920,format=rgba,alphaextract[ma];"
                f"[vcore][ma]alphamerge[va];"
                f"color=c={BG_COLOR}:s=1080x1920[bg];"
                f"[bg][va]overlay=format=auto[masked];"
                f"[masked][2:v]overlay={lx}:{ly}"
                f"{text_suffix}"
                f"[final]"
            )
            cmd_insta = [
                ffmpeg_cmd, "-y",
                "-i", str(video_path),
                "-i", str(mask_path),
                "-i", str(logo_ready_path),
                "-filter_complex", filter_complex,
                "-map", "[final]", "-map", "0:a?",
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(insta_post)
            ]
        else:
            filter_complex = (
                f"[0:v]{base_transform_snippet},format=rgba[vcore];"
                f"[1:v]scale=1080:1920,format=rgba,alphaextract[ma];"
                f"[vcore][ma]alphamerge[va];"
                f"color=c={BG_COLOR}:s=1080x1920[bg];"
                f"[bg][va]overlay=format=auto"
                f"{text_suffix}"
                f"[final]"
            )
            cmd_insta = [
                ffmpeg_cmd, "-y",
                "-i", str(video_path),
                "-i", str(mask_path),
                "-filter_complex", filter_complex,
                "-map", "[final]", "-map", "0:a?",
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(insta_post)
            ]
    else:
        # no frame/mask
        if logo_ready_path and logo_ready_path.exists():
            # Inputs: 0=video, 1=logo
            filter_complex = (
                f"[0:v]{base_transform_snippet}[vcore];"
                f"[vcore][1:v]overlay={lx}:{ly}"
                f"{text_suffix}"
                f"[final]"
            )
            cmd_insta = [
                ffmpeg_cmd, "-y",
                "-i", str(video_path),
                "-i", str(logo_ready_path),
                "-filter_complex", filter_complex,
                "-map", "[final]", "-map", "0:a?",
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(insta_post)
            ]
        else:
            vf_final = f"{base_transform_snippet}{text_suffix}"
            cmd_insta = [
                ffmpeg_cmd, "-y",
                "-i", str(video_path),
                "-vf", vf_final,
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(insta_post)
            ]

    ok, err = run_ffmpeg(cmd_insta)
    if not ok:
        return False, {"error": f"ffmpeg instagram_post failed: {err}", "slug": slug}

    # 2) Feed 1080x1080 (basit yol; istersen buraya da çerçeve+logo ekleyebiliriz)
    if produce_feed:
        vf_feed = (
            "scale=1080:-2,"
            "pad=width='max(1080,iw)':height='max(1080,ih)':x='(ow-iw)/2':y='(oh-ih)/2':color=black,"
            "crop=1080:1080"
        )
        feed_chain = f"{vf_feed}{text_suffix}"
        cmd_feed = [
            ffmpeg_cmd, "-y",
            "-i", str(video_path),
            "-vf", feed_chain,
            *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
            "-c:a", "aac", "-b:a", "128k",
            str(feed_out)
        ]
        ok, err = run_ffmpeg(cmd_feed)
        if not ok:
            return False, {"error": f"ffmpeg feed failed: {err}", "slug": slug}

    # 3) Reels 1080x1920 (aynı mantık, logo dahil)
    if produce_reels:
        if use_frame and frame_path.exists():
            if logo_ready_path and logo_ready_path.exists():
                filter_complex = (
                    f"[0:v]{base_transform_snippet}[vcore];"
                    f"[vcore][1:v]overlay=0:0[withframe];"
                    f"[withframe][2:v]overlay={lx}:{ly}"
                    f"{text_suffix}"
                    f"[final]"
                )
                cmd_reels = [
                    ffmpeg_cmd, "-y",
                    "-i", str(video_path),
                    "-i", str(frame_path),
                    "-i", str(logo_ready_path),
                    "-filter_complex", filter_complex,
                    "-map", "[final]", "-map", "0:a?",
                    *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                    "-c:a", "aac", "-b:a", "128k",
                    str(reels_out)
                ]
            else:
                filter_complex = (
                    f"[0:v]{base_transform_snippet}[vcore];"
                    f"[vcore][1:v]overlay=0:0"
                    f"{text_suffix}"
                    f"[final]"
                )
                cmd_reels = [
                    ffmpeg_cmd, "-y",
                    "-i", str(video_path),
                    "-i", str(frame_path),
                    "-filter_complex", filter_complex,
                    "-map", "[final]", "-map", "0:a?",
                    *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                    "-c:a", "aac", "-b:a", "128k",
                    str(reels_out)
                ]
        elif use_mask and mask_path.exists():
            if logo_ready_path and logo_ready_path.exists():
                filter_complex = (
                    f"[0:v]{base_transform_snippet},format=rgba[vcore];"
                    f"[1:v]scale=1080:1920,format=rgba,alphaextract[ma];"
                    f"[vcore][ma]alphamerge[va];"
                    f"color=c={BG_COLOR}:s=1080x1920[bg];"
                    f"[bg][va]overlay=format=auto[masked];"
                    f"[masked][2:v]overlay={lx}:{ly}"
                    f"{text_suffix}"
                    f"[final]"
                )
                cmd_reels = [
                    ffmpeg_cmd, "-y",
                    "-i", str(video_path),
                    "-i", str(mask_path),
                    "-i", str(logo_ready_path),
                    "-filter_complex", filter_complex,
                    "-map", "[final]", "-map", "0:a?",
                    *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                    "-c:a", "aac", "-b:a", "128k",
                    str(reels_out)
                ]
            else:
                filter_complex = (
                    f"[0:v]{base_transform_snippet},format=rgba[vcore];"
                    f"[1:v]scale=1080:1920,format=rgba,alphaextract[ma];"
                    f"[vcore][ma]alphamerge[va];"
                    f"color=c={BG_COLOR}:s=1080x1920[bg];"
                    f"[bg][va]overlay=format=auto"
                    f"{text_suffix}"
                    f"[final]"
                )
                cmd_reels = [
                    ffmpeg_cmd, "-y",
                    "-i", str(video_path),
                    "-i", str(mask_path),
                    "-filter_complex", filter_complex,
                    "-map", "[final]", "-map", "0:a?",
                    *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                    "-c:a", "aac", "-b:a", "128k",
                    str(reels_out)
                ]
        else:
            if logo_ready_path and logo_ready_path.exists():
                filter_complex = (
                    f"[0:v]{base_transform_snippet}[vcore];"
                    f"[vcore][1:v]overlay={lx}:{ly}"
                    f"{text_suffix}"
                    f"[final]"
                )
                cmd_reels = [
                    ffmpeg_cmd, "-y",
                    "-i", str(video_path),
                    "-i", str(logo_ready_path),
                    "-filter_complex", filter_complex,
                    "-map", "[final]", "-map", "0:a?",
                    *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                    "-c:a", "aac", "-b:a", "128k",
                    str(reels_out)
                ]
            else:
                vf_reels_final = f"{base_transform_snippet}{text_suffix}"
                cmd_reels = [
                    ffmpeg_cmd, "-y",
                    "-i", str(video_path),
                    "-vf", vf_reels_final,
                    *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
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
            cmd_wm = [
                ffmpeg_cmd, "-y",
                "-i", str(reels_out), "-i", str(watermark),
                "-filter_complex", "overlay=10:10",
                *venc_args, "-pix_fmt", "yuv420p", "-movflags", "+faststart",
                "-c:a", "aac", "-b:a", "128k",
                str(reels_wm)
            ]
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
        "meta": str(downloads_dir / f"{slug}.json")
    }