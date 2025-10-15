from __future__ import annotations

import os
import json
import shutil
import subprocess
from pathlib import Path
from typing import Optional, Tuple

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

def run_ffmpeg(cmd_args: list[str]):
    try:
        subprocess.run(cmd_args, check=True)
        return True, None
    except subprocess.CalledProcessError as e:
        return False, e
    except Exception as e:
        return False, e

def ffmpeg_escape_text(txt: str) -> str:
    if not txt:
        return ""
    return txt.replace("\\", "\\\\").replace("'", "\\'").replace(":", "\\:").replace("%", "%%")

def ffmpeg_escape_fontfile(path: str) -> str:
    return path.replace("\\", "\\\\").replace("'", "\\'")

def nvenc_available(ffmpeg_cmd: str) -> bool:
    """
    Returns True if h264_nvenc encoder is available in this ffmpeg build.
    """
    try:
        res = subprocess.run([ffmpeg_cmd, "-hide_banner", "-encoders"], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, check=True)
        return "h264_nvenc" in res.stdout
    except Exception:
        return False