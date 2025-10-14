#!/usr/bin/env python3
"""
download_manager_limit.py

- Searched hits içinden en az min-duration (varsayılan 8s) olan ve download URL'si olan klipleri bularak indirir.
- --max-concurrent  ile aynı anda kaç indirme yapılacağını sınırlar (thread pool).
- --max-total ile toplam kaç klip indirileceğini sınırlar.
- İndirilen dosyaları ffprobe ile doğrular; gerçek süre < min-duration ise dosya silinir ve bir sonraki uygun klip denenir.
- FFPROBE yolu şu öncelikle bulunur:
    1) CLI argümanı --ffprobe-path
    2) Env var FFPROBE_PATH
    3) shutil.which('ffprobe')
    4) Windows WinGet default: %USERPROFILE%\AppData\Local\Microsoft\WinGet\Links\ffprobe.exe
  Bulunan tam yol kullanılarak subprocess çağrılır.

Kullanım örneği:
export CLIPC_CAFE_API_KEY="YOUR_KEY"
python download_manager_limit.py --actor "Tom Hardy" --min-duration 8 --size 100 --max-concurrent 5 --max-total 5

Ya da ffprobe yolunu belirt:
python download_manager_limit.py --ffprobe-path "C:\\Users\\0meer\\AppData\\Local\\Microsoft\\WinGet\\Links\\ffprobe.exe" ...
"""
import os
import sys
import json
import argparse
import requests
from pathlib import Path
import subprocess
import shutil
import concurrent.futures
import threading
import time

# optional dotenv
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

API_KEY = os.getenv("CLIPC_CAFE_API_KEY") or os.getenv("API_KEY")
if not API_KEY:
    print("Error: CLIPC_CAFE_API_KEY not set. Create a .env or export the env var.")
    sys.exit(1)

BASE_URL = "https://api.clip.cafe/"

def find_ffprobe(cli_arg_path=None):
    # 1) CLI arg
    if cli_arg_path:
        p = Path(cli_arg_path)
        if p.exists():
            return str(p)
        # try as-is (maybe path to folder)
        if (p / "ffprobe.exe").exists():
            return str((p / "ffprobe.exe").resolve())
    # 2) Env var
    env_path = os.environ.get("FFPROBE_PATH")
    if env_path:
        p = Path(env_path)
        if p.exists():
            return str(p)
    # 3) shutil.which
    which_path = shutil.which("ffprobe")
    if which_path:
        return which_path
    # 4) Windows WinGet default location
    if os.name == "nt":
        userprofile = os.environ.get("USERPROFILE")
        if userprofile:
            candidate = Path(userprofile) / "AppData" / "Local" / "Microsoft" / "WinGet" / "Links" / "ffprobe.exe"
            if candidate.exists():
                return str(candidate)
    # not found
    return None

def ffprobe_duration(path: Path, ffprobe_cmd: str):
    """
    Run ffprobe using the explicit ffprobe_cmd (full path or 'ffprobe') and return duration as float or None.
    """
    try:
        cmd = [ffprobe_cmd, "-v", "error", "-show_entries", "format=duration",
               "-of", "default=noprint_wrappers=1:nokey=1", str(path)]
        res = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
        out = res.stdout.strip()
        return float(out) if out else None
    except subprocess.CalledProcessError as e:
        # include stderr for debugging
        # print("ffprobe returned error:", e.stderr)
        return None
    except Exception:
        return None

def safe_filename_from_slug(slug: str):
    name = (slug or "").replace("/", "_").replace(" ", "_")
    return "".join(c for c in name if c.isalnum() or c in "-_.").strip("-_") or f"clip_{int(time.time())}"

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

def download_job(dl_url: str, out_file: Path, meta: dict, min_duration: int, ffprobe_cmd: str):
    """
    Downloads dl_url to out_file; validates ffprobe duration >= min_duration using ffprobe_cmd.
    Returns tuple (success:bool, info)
    """
    tmp_file = out_file.with_suffix(".part")
    try:
        with requests.get(dl_url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(tmp_file, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        shutil.move(str(tmp_file), str(out_file))
    except Exception as e:
        # cleanup partial
        try:
            if tmp_file.exists():
                tmp_file.unlink()
        except:
            pass
        return False, f"download error: {e}"

    dur = ffprobe_duration(out_file, ffprobe_cmd)
    if dur is None:
        # if ffprobe failed, remove file to avoid storing unknown content
        try:
            out_file.unlink()
        except:
            pass
        return False, "ffprobe missing or failed"
    if dur >= min_duration:
        return True, dur
    else:
        # remove too-short file
        try:
            out_file.unlink()
        except:
            pass
        return False, dur

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--actor", default="Tom Hardy", help="Actor name to search")
    parser.add_argument("--min-duration", type=int, default=8, help="Minimum clip duration seconds (inclusive)")
    parser.add_argument("--size", type=int, default=100, help="How many search results to request (max per search call)")
    parser.add_argument("--max-concurrent", type=int, default=5, help="Max concurrent downloads")
    parser.add_argument("--max-total", type=int, default=5, help="Max total downloads to perform")
    parser.add_argument("--outdir", default="downloads", help="Output directory")
    parser.add_argument("--offset", type=int, default=0, help="Search offset (for pagination)")
    parser.add_argument("--ffprobe-path", default=None, help="Explicit path to ffprobe executable")
    args = parser.parse_args()

    actor = args.actor
    min_duration = args.min_duration
    size = args.size
    max_concurrent = max(1, args.max_concurrent)
    max_total = max(1, args.max_total)
    outdir = Path(args.outdir)
    outdir.mkdir(parents=True, exist_ok=True)
    offset = args.offset

    # determine ffprobe
    ffprobe_cmd = find_ffprobe(args.ffprobe_path)
    if not ffprobe_cmd:
        print("ERROR: ffprobe not found. Set FFPROBE_PATH env var, pass --ffprobe-path, or add ffprobe to PATH.")
        print("If you installed via WinGet the typical path is:")
        print(r"  C:\Users\<yourname>\AppData\Local\Microsoft\WinGet\Links\ffprobe.exe")
        print("Example (PowerShell):")
        print(r'  $env:FFPROBE_PATH="C:\Users\<yourname>\AppData\Local\Microsoft\WinGet\Links\ffprobe.exe"')
        print("Or run the script with --ffprobe-path \"C:\\path\\to\\ffprobe.exe\"")
        return

    print("Using ffprobe at:", ffprobe_cmd)
    print(f"Searching for actor='{actor}', duration>={min_duration}s, size={size}, offset={offset}")
    js, err = do_search(actor, min_duration, size, offset=offset)
    if err:
        print("Search error or non-200 response (server text/snippet):")
        print(err[:2000])
        return

    hits = js.get("hits", {}).get("hits", [])
    if not hits:
        print("No hits returned. Try removing duration filter or use different actor/movie_title.")
        return

    print(f"Found {len(hits)} hits in this page. Will attempt up to {max_total} downloads with up to {max_concurrent} concurrent workers.")

    futures = []
    downloaded_count = 0

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        for idx, h in enumerate(hits):
            if downloaded_count >= max_total:
                break

            src = h.get("_source", {}) or {}
            slug = src.get("slug") or src.get("title") or f"clip_{idx}"
            meta_dur = src.get("duration")
            try:
                meta_dur_int = int(meta_dur) if meta_dur is not None else None
            except:
                meta_dur_int = None

            # Skip if metadata says too short
            if meta_dur_int is not None and meta_dur_int < min_duration:
                print(f"[{idx}] Skipping (metadata duration < required): slug={slug} meta_dur={meta_dur_int}")
                continue

            dl = src.get("download")
            if not dl:
                print(f"[{idx}] Skipping (no download URL): slug={slug}")
                continue

            # prepare output file
            safe = safe_filename_from_slug(slug)
            out_file = outdir / f"{safe}.mp4"
            meta_file = outdir / f"{safe}.json"
            with open(meta_file, "w", encoding="utf-8") as f:
                json.dump(src, f, ensure_ascii=False, indent=2)

            # submit download task
            print(f"[{idx}] Submitting download task: slug={slug}")
            future = executor.submit(download_job, dl, out_file, src, min_duration, ffprobe_cmd)
            futures.append((future, slug, out_file, meta_file))

            downloaded_count += 1
            if downloaded_count >= max_total:
                print("Reached max_total queued downloads.")
                break

        # wait for all queued futures to complete and report results
        print("Waiting for queued downloads to finish...")
        success_count = 0
        for fut, slug, out_file, meta_file in futures:
            try:
                ok, info = fut.result()
                if ok:
                    success_count += 1
                    print(f"SUCCESS: {slug} saved -> {out_file} (duration {info}s)")
                else:
                    print(f"FAILED: {slug} -> {info}")
            except Exception as e:
                print(f"ERROR: future for {slug} raised exception: {e}")

        print(f"All queued downloads processed. Successes: {success_count}/{len(futures)}")
        if success_count == 0:
            print("No successful downloads. Try increasing 'size' or using a different search query (movie_title etc.).")

if __name__ == "__main__":
    main()