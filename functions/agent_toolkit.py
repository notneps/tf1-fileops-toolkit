import os
import shutil
from datetime import datetime
from zoneinfo import ZoneInfo
import os, re
from typing import List, Tuple
import csv

def create_gallery(folder, output="gallery.html"):
    # Valid image extensions
    exts = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".webp")
    # Sort files by name
    files = sorted([f for f in os.listdir(folder) if f.lower().endswith(exts)])

    # Start HTML
    html = ["<html><head><style>"]
    html.append("body { font-family: sans-serif; }")
    html.append(".row { display: flex; margin-bottom: 20px; }")
    html.append(".item { flex: 1; text-align: center; margin-right: 10px; }")
    html.append(".item img { max-width: 100%; border: 1px solid #ccc; display: block; margin: auto; }")
    html.append(".filename { font-size: 14px; margin-top: 5px; color: #333; word-break: break-word; }")
    html.append("</style></head><body>")
    html.append("<h1>Image Gallery</h1>")

    # Group images in threes
    for i in range(0, len(files), 3):
        html.append('<div class="row">')
        for f in files[i:i+3]:
            path = os.path.join(folder, f).replace("\\", "/")  # safe for browser
            html.append('<div class="item">')
            html.append(f'<img src="{path}">')
            html.append(f'<div class="filename">{f}</div>')
            html.append('</div>')
        html.append('</div>')

    html.append("</body></html>")

    with open(output, "w", encoding="utf-8") as f:
        f.write("\n".join(html))

    print(f"✅ Gallery saved to {output}. Open it in your browser.")

def folderize(target_folder,
              folder_name_override=False,
              log_file="folderize_log.txt"):
    
    #print(f"DEBUG: target_folder type={type(target_folder)}, value={target_folder}")

    #folder_name_override = False

    base_dir = target_folder  # use the arg

    # --- Logging Setup ---
    if log_file is None:
        script_dir = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(script_dir, "logs")
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, "folderize_log.txt")

    def log(msg):
        print(msg)
        with open(log_file, "a", encoding="utf-8") as logf:
            logf.write(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - {msg}\n")

    # --- Helpers ---
    def parse_folder_key(filename: str):
        """
        Accepts:
          - IMG_LANG_PID_SEQ             e.g., IMG_FI_20001_1
          - IMG_LANG_REGION_PID_SEQ      e.g., IMG_ZH_TW_20001_1
        Returns folder key like 'FI_20001' or 'ZH_TW_20001', otherwise None.
        """
        if not (filename.startswith("IMG_") and "_" in filename):
            return None

        parts = filename.split("_")
        # Basic guard
        if len(parts) < 4:
            return None

        # Case A: two-part language (e.g., ZH_TW)
        # parts: [IMG, ZH, TW, 20001, 1]
        if len(parts) >= 5 and parts[2].isalpha() and parts[1].isalpha():
            lang = f"{parts[1]}_{parts[2]}"
            pid = parts[3]
            return f"{lang}_{pid}"

        # Case B: one-part language (e.g., FI)
        # parts: [IMG, FI, 20001, 1]
        if parts[1].isalpha():
            lang = parts[1]
            pid = parts[2]
            return f"{lang}_{pid}"

        return None

    log("----- Sorting Started -----")

    # --- Initialize Counters/Map ---
    total_files = 0
    total_grouped = 0
    total_skipped = 0
    folder_file_map = {}

    # --- Collect Files ---
    for filename in os.listdir(base_dir):
        file_path = os.path.join(base_dir, filename)
        if os.path.isdir(file_path):
            continue

        folder_key = parse_folder_key(filename)
        if folder_key:
            folder_file_map.setdefault(folder_key, []).append(filename)
            total_files += 1
        else:
            log(f"⚠️ Skipping unrecognized file: {filename}")

    # --- Process Folder Groups ---
    for folder_name, files in folder_file_map.items():
        if len(files) == 3:
            folder_path = os.path.join(base_dir, folder_name)
            if not os.path.exists(folder_path):
                log(f"📁 Creating folder: {folder_name}")
                os.makedirs(folder_path)
            for f in files:
                src = os.path.join(base_dir, f)
                dst = os.path.join(folder_path, f)
                log(f"📦 Moving {f} -> {folder_name}")
                shutil.move(src, dst)
            total_grouped += 1
        else:
            log(f"❌ Skipping {folder_name}: only found {len(files)}/3 files.")
            total_skipped += 1

    # --- Final Stats ---
    log("----- Sorting Complete -----")
    log(f"📊 Total IMG_ files found: {total_files}")
    log(f"📦 Folders grouped (3 files each): {total_grouped}")
    log(f"❌ Skipped groups (not exactly 3 files): {total_skipped}")
    print("✅ Grouping Done!")

    # --- Move PID folders into a date folder (Eastern Time, yyyymmdd) ---
    eastern_time = datetime.now(ZoneInfo("America/New_York"))
    today_str = eastern_time.strftime("%Y%m%d")

    if folder_name_override:
        today_str = folder_name_override
        log(f"Date folder manually set to {today_str}")

    date_folder_path = os.path.join(base_dir, today_str)
    os.makedirs(date_folder_path, exist_ok=True)

    log(f"📂 Moving PID folders into date folder: {today_str}")
    moved_count = 0
    for folder_name, files in folder_file_map.items():
        folder_path = os.path.join(base_dir, folder_name)
        if os.path.isdir(folder_path) and len(files) == 3:
            dst = os.path.join(date_folder_path, folder_name)
            shutil.move(folder_path, dst)
            moved_count += 1

    log(f"✅ Moved {moved_count} PID folders to {today_str}")
    print("✅ Done!")


def filenamerize(
    target_dir,
    language_code,
    start_pid,
    sort_by,
    prefix="IMG_",
    files_per_asset=3,
    dry_run=False
):
    TARGET_DIR = target_dir
    LANGUAGE_CODE = language_code
    START_PID = start_pid
    SORT_BY = sort_by
    PREFIX = prefix
    FILES_PER_ASSET = files_per_asset

    # --- helpers ---
    def natural_key(s: str):
        return [int(t) if t.isdigit() else t.lower()
                for t in re.findall(r'\d+|\D+', s)]

    def exif_datetime_key(path: str):
        # EXIF DateTimeOriginal; fallback to mtime
        try:
            from PIL import Image, ExifTags
            with Image.open(path) as im:
                exif = im.getexif() or {}
                tag_id = next((k for k, v in ExifTags.TAGS.items() if v == "DateTimeOriginal"), None)
                if tag_id and tag_id in exif:
                    s = str(exif[tag_id])  # "YYYY:MM:DD HH:MM:SS"
                    date, time = s.split()
                    y, m, d = map(int, date.split(":"))
                    hh, mm, ss = map(int, time.split(":"))
                    return (y, m, d, hh, mm, ss)
        except Exception:
            pass
        return (os.path.getmtime(path),)

    def sort_files(files: List[str], folder: str, how: str) -> List[str]:
        if how == "date":
            files.sort(key=lambda f: os.path.getmtime(os.path.join(folder, f)))
        elif how == "name":
            files.sort()
        elif how == "name_natural":
            files.sort(key=natural_key)
        elif how == "date_exif":
            files.sort(key=lambda f: exif_datetime_key(os.path.join(folder, f)))
        else:
            raise ValueError("Invalid SORT_BY. Use 'date', 'name', 'name_natural', or 'date_exif'.")
        return files

    def compute_new(idx: int, filename: str) -> Tuple[str, str, int, int]:
        pid = START_PID + (idx // FILES_PER_ASSET)
        image_num = (idx % FILES_PER_ASSET) + 1
        ext = os.path.splitext(filename)[1]
        new_name = f"{PREFIX}{LANGUAGE_CODE}_{pid}_{image_num}{ext}"
        return new_name, ext, pid, image_num

    # --- gather ---
    files = [f for f in os.listdir(TARGET_DIR) if os.path.isfile(os.path.join(TARGET_DIR, f))]
    total = len(files)
    print(f"\n📊 Total files found (regular files only): {total}")
    if total == 0:
        print("❌ No files found. Aborting.")
        return

    files = sort_files(files, TARGET_DIR, SORT_BY)  # matches your preview+rename order 1:1  :contentReference[oaicite:1]{index=1}

    # --- improved preview ---
    print(f"🔽 Sorted by: {SORT_BY}")
    head, tail = files[:5], files[-5:] if total > 5 else []
    print("\n👀 First files:")
    for i, f in enumerate(head, 1): print(f"   {i:>2}. {f}")
    if tail:
        print("\n👀 Last files:")
        base = total - len(tail) + 1
        for i, f in enumerate(tail, base): print(f"   {i:>2}. {f}")

    # show dry-run mapping for first/last few
    def show_map(sample: List[Tuple[int, str]]):
        for i, f in sample:
            new_name, _, pid, num = compute_new(i, f)
            print(f"   [{i+1:>3}] {f}  →  {new_name}    (PID {pid}, #{num})")

    print("\n🔒 Dry-run mapping (first 10):")
    show_map(list(enumerate(files[:10])))
    if total > 20:
        print("   …")
    if total > 10:
        print("🔒 Dry-run mapping (last 10):")
        start = total - 10
        show_map([(i, files[i]) for i in range(start, total)])

    # confirm
    if not dry_run:
        confirm = input("\n❓ Proceed with renaming? (y/n): ").strip().lower()
        if confirm != "y":
            print("❌ Operation cancelled.")
            return
    else:
        print("🧪 Dry-run enabled: no files will be renamed.")
        return

    # --- rename with safeguards ---
    success = 0
    skipped_same = 0
    skipped_collision = 0
    failed = 0

    for idx, filename in enumerate(files):
        old_path = os.path.join(TARGET_DIR, filename)
        new_name, _, _, _ = compute_new(idx, filename)
        new_path = os.path.join(TARGET_DIR, new_name)

        # skip if already the correct name
        if os.path.normcase(old_path) == os.path.normcase(new_path):
            print(f"⏭️  Already named correctly, skipping: {filename}")
            skipped_same += 1
            continue

        # collision check
        if os.path.exists(new_path):
            print(f"⚠️  Destination exists, skipping: {filename} → {new_name}")
            skipped_collision += 1
            continue

        try:
            os.rename(old_path, new_path)  # original behavior :contentReference[oaicite:2]{index=2}
            print(f"🔁 {filename} → {new_name}")
            success += 1
        except Exception as e:
            print(f"❌ Failed on {filename}: {e}")
            failed += 1
            continue

    print("\n✅ Done.")
    print(f"   Renamed   : {success}")
    print(f"   Skipped✓  : {skipped_same} (already correct)")
    print(f"   Skipped⚠  : {skipped_collision} (name existed)")
    print(f"   Failed    : {failed}")


def extract_files_from_pid(base_dir, folder_prefix): 
    
    target_folder = os.path.join(base_dir, "COLLECTED_FILES")    

    # --- Setup target folder ---
    os.makedirs(target_folder, exist_ok=True)

    # --- Move files ---
    moved_files = 0
    for entry in os.listdir(base_dir):
        pid_path = os.path.join(base_dir, entry)
        
        # Check for PID folders starting with 
        if os.path.isdir(pid_path) and entry.startswith(folder_prefix):
            for root, _, files in os.walk(pid_path):
                for file in files:
                    src_file = os.path.join(root, file)
                    dest_file = os.path.join(target_folder, file)

                    # Handle filename conflicts
                    name, ext = os.path.splitext(file)
                    counter = 1
                    while os.path.exists(dest_file):
                        dest_file = os.path.join(target_folder, f"{name}_{counter}{ext}")
                        counter += 1

                    shutil.move(src_file, dest_file)
                    moved_files += 1

    print(f"✅ Moved {moved_files} file(s) into: {target_folder}")


import os
import hashlib
import shutil

def file_md5(filepath, chunk_size=8192):
    """Compute MD5 hash of a file."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()


def revert_original_filenames(raw_dir, renamed_dir, log_csv="restore_log.csv"):
    """
    Restore original filenames in renamed_dir using MD5 hashes
    from files in raw_dir, and log the changes to a CSV file.
    
    Args:
        raw_dir (str): Directory containing original files.
        renamed_dir (str): Directory containing renamed files to be restored.
        log_csv (str): Path to the CSV log file (default: restore_log.csv).
    """
    # 1) Build a map of md5 -> original filename
    md5_to_name = {}
    for root, _, files in os.walk(raw_dir):
        for fname in files:
            path = os.path.join(root, fname)
            file_hash = file_md5(path)
            md5_to_name[file_hash] = fname

    # Open CSV for logging
    with open(log_csv, mode="w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["md5", "old_filename", "new_filename"])

        # 2) For each file in renamed_dir, match and rename
        for root, _, files in os.walk(renamed_dir):
            for fname in files:
                path = os.path.join(root, fname)
                file_hash = file_md5(path)
                
                if file_hash in md5_to_name:
                    original_name = md5_to_name[file_hash]
                    new_path = os.path.join(root, original_name)
                    
                    if path != new_path:  # Avoid unnecessary rename
                        # Handle filename collisions by appending suffix
                        counter = 1
                        base, ext = os.path.splitext(original_name)
                        while os.path.exists(new_path):
                            new_path = os.path.join(root, f"{base}_{counter}{ext}")
                            counter += 1
                        
                        print(f"Renaming: {fname} -> {os.path.basename(new_path)}")
                        shutil.move(path, new_path)

                        # Log the change
                        writer.writerow([file_hash, fname, os.path.basename(new_path)])
                else:
                    print(f"⚠️ No match found for {fname} (hash={file_hash})")
                    writer.writerow([file_hash, fname, "NO_MATCH"])