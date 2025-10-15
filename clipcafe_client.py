from __future__ import annotations

import os
import sys
import json
import shutil
import requests
from pathlib import Path
from typing import Optional, Tuple

# optional dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

BASE_URL = "https://api.clip.cafe/"

def get_api_key() -> Optional[str]:
    return os.getenv("CLIPC_CAFE_API_KEY") or os.getenv("API_KEY")

def do_search(actor: str, min_duration: int, size: int, offset: int = 0):
    """
    Search clips via clip.cafe API.
    Returns (json, None) on success or (None, error_str) on failure.
    """
    api_key = get_api_key()
    if not api_key:
        return None, "CLIPC_CAFE_API_KEY not set. Export it or put in .env."

    params = {
        "api_key": api_key,
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

def download_stream(dl_url: str, out_path: Path, timeout: int = 120) -> Tuple[bool, Optional[str]]:
    """
    Stream download to a .part and move to final path atomically.
    """
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