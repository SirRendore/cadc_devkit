#!/usr/bin/env python

import argparse
import os
import sys
import time
import zipfile
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# CADCD = {
#     '2018_03_06': [
#         '0001','0002','0005','0006','0008','0009','0010',
#         '0012','0013','0015','0016','0018'
#     ],
#     '2018_03_07': [
#         '0001','0002','0004','0005','0006','0007'
#     ],
#     '2019_02_27': [
#         '0002','0003','0004','0005','0006','0008','0009','0010',
#         '0011','0013','0015','0016','0018','0019','0020',
#         '0022','0024','0025','0027','0028','0030',
#         '0031','0033','0034','0035','0037','0039','0040',
#         '0041','0043','0044','0045','0046','0047','0049','0050',
#         '0051','0054','0055','0056','0058','0059',
#         '0060','0061','0063','0065','0066','0068','0070',
#         '0072','0073','0075','0076','0078','0079',
#         '0080','0082'
#     ]
# }



CADCD = {
    '2018_03_06': [
        '0018'
    ],
    '2019_02_27': [
        '0056','0061',
    ]
}



BASE_URL = "http://wiselab.uwaterloo.ca/cadcd_data"


def make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=8,
        connect=8,
        read=8,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET", "HEAD"),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=4, pool_maxsize=4)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session


def zip_ok(path: Path) -> bool:
    try:
        with zipfile.ZipFile(path) as z:
            return z.testzip() is None
    except Exception:
        return False


def human(n: int) -> str:
    # simple human-readable bytes
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if n < 1024:
            return f"{n:.0f}{unit}"
        n /= 1024
    return f"{n:.0f}PB"


def download_atomic(
    session: requests.Session,
    url: str,
    final_path: Path,
    *,
    resume: bool = True,
    timeout=(10, 60),
    chunk_size: int = 1024 * 1024,
    progress_every_bytes: int = 25 * 1024 * 1024,  # print every ~25MB
) -> None:
    """
    Download url to final_path safely:
    - writes to final_path.suffix + ".part"
    - resumes if .part exists and server supports Range
    - renames atomically to final_path when complete
    """
    final_path = Path(final_path)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    tmp_path = final_path.with_suffix(final_path.suffix + ".part")

    # If we already have a complete-looking final file, keep it.
    # Caller can validate (e.g., zip_ok) and delete if needed.
    if final_path.exists():
        return

    existing = tmp_path.stat().st_size if (resume and tmp_path.exists()) else 0
    headers = {}
    mode = "ab" if existing > 0 else "wb"
    if existing > 0:
        headers["Range"] = f"bytes={existing}-"

    with session.get(url, stream=True, headers=headers, timeout=timeout) as r:
        # If server ignored Range, it might return 200 with full content.
        if existing > 0 and r.status_code == 200:
            # restart cleanly
            tmp_path.unlink(missing_ok=True)
            return download_atomic(
                session, url, final_path,
                resume=resume, timeout=timeout, chunk_size=chunk_size,
                progress_every_bytes=progress_every_bytes
            )

        r.raise_for_status()

        total = None
        # For non-ranged responses, Content-Length is full size.
        # For ranged, Content-Length is remaining bytes; still useful for progress.
        cl = r.headers.get("Content-Length")
        if cl is not None:
            try:
                total = int(cl) + existing
            except ValueError:
                total = None

        downloaded = existing
        last_print_at = existing
        start = time.time()

        with open(tmp_path, mode) as f:
            for chunk in r.iter_content(chunk_size=chunk_size):
                if not chunk:
                    continue
                f.write(chunk)
                downloaded += len(chunk)

                if downloaded - last_print_at >= progress_every_bytes:
                    if total:
                        pct = 100.0 * downloaded / total
                        print(f"    ... {human(downloaded)}/{human(total)} ({pct:.1f}%)")
                    else:
                        print(f"    ... {human(downloaded)}")
                    last_print_at = downloaded

    # If server provided an expected size (total), enforce it.
    if total is not None:
        actual = tmp_path.stat().st_size
        if actual != total:
            tmp_path.unlink(missing_ok=True)
            raise IOError(f"Incomplete download for {url}: expected {total}, got {actual}")

    # Atomic rename -> final_path
    os.replace(tmp_path, final_path)

    elapsed = time.time() - start
    if elapsed > 0:
        print(f"    done: {final_path.name} ({human(final_path.stat().st_size)}) in {elapsed:.1f}s")


def download_zip_and_extract(
    session: requests.Session,
    url: str,
    out_dir: Path,
    zip_name: str,
    *,
    resume: bool = True,
    keep_zip: bool = False,
) -> None:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    zip_path = out_dir / zip_name

    # If zip exists but is corrupt/truncated, delete it and re-download
    if zip_path.exists() and not zip_ok(zip_path):
        print(f"    existing zip is corrupt/incomplete, deleting: {zip_path}")
        zip_path.unlink(missing_ok=True)

    if not zip_path.exists():
        print(f"    downloading: {url}")
        download_atomic(session, url, zip_path, resume=resume)

        # validate zip
        if not zip_ok(zip_path):
            zip_path.unlink(missing_ok=True)
            raise IOError(f"Downloaded zip is still corrupt/incomplete: {zip_path}")

    # Extract
    print(f"    extracting: {zip_path.name} -> {out_dir}")
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(out_dir)

    if not keep_zip:
        zip_path.unlink(missing_ok=True)


def download_file(
    session: requests.Session,
    url: str,
    out_path: Path,
    *,
    resume: bool = True,
) -> None:
    out_path = Path(out_path)
    if out_path.exists():
        return
    print(f"    downloading: {url}")
    download_atomic(session, url, out_path, resume=resume)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dataset_path", default=r"/home/zhaohaoa/cadc", help="Root folder to store dataset")
    ap.add_argument("--labeled", action="store_true", help="Download labeled data (if available)")
    ap.add_argument("--no-resume", action="store_true", help="Disable resume support")
    ap.add_argument("--keep-zips", action="store_true", help="Keep downloaded zip files")
    args = ap.parse_args()

    dataset_path = Path(args.dataset_path)
    root_dir = dataset_path / ("cadcd_labeled" if args.labeled else "cadcd_raw")
    root_dir.mkdir(parents=True, exist_ok=True)

    print("Downloading labeled data" if args.labeled else "Downloading raw data")
    session = make_session()
    resume = not args.no_resume

    for date, drives in CADCD.items():
        print(f"\n== {date} ==")
        date_dir = root_dir / date
        date_dir.mkdir(parents=True, exist_ok=True)

        # Calibration zip for this date
        calib_url = f"{BASE_URL}/{date}/calib.zip"
        try:
            download_zip_and_extract(
                session,
                calib_url,
                date_dir,
                "calib.zip",
                resume=resume,
                keep_zip=args.keep_zips,
            )
        except Exception as e:
            print(f"ERROR downloading/extracting calib for {date}: {e}", file=sys.stderr)
            continue

        for drive in drives:
            print(f"  -- drive {drive} --")
            drive_dir = date_dir / drive
            drive_dir.mkdir(parents=True, exist_ok=True)

            base = f"{BASE_URL}/{date}/{drive}"

            try:
                if args.labeled:
                    # labeled.zip + 3d_ann.json
                    data_url = f"{base}/labeled.zip"
                    ann_url = f"{base}/3d_ann.json"

                    download_zip_and_extract(
                        session,
                        data_url,
                        drive_dir,
                        "labeled.zip",
                        resume=resume,
                        keep_zip=args.keep_zips,
                    )
                    download_file(session, ann_url, drive_dir / "3d_ann.json", resume=resume)
                else:
                    data_url = f"{base}/raw.zip"
                    download_zip_and_extract(
                        session,
                        data_url,
                        drive_dir,
                        "raw.zip",
                        resume=resume,
                        keep_zip=args.keep_zips,
                    )

            except Exception as e:
                print(f"ERROR on {date}/{drive}: {e}", file=sys.stderr)
                # If a partial .part is left behind, next run can resume it (unless --no-resume).
                continue

    print("\nAll done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
