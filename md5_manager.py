import os
import hashlib
import pickle
import shutil
import time
from datetime import datetime, timezone#, timedelta
from typing import Set, Tuple

import pandas as pd
from tqdm import tqdm

# ==============================
# Registry Management
# ==============================

def load_registry_v1(pickle_path: str) -> pd.DataFrame:
    """Load existing registry from pickle, or create empty DataFrame if not found."""
    if os.path.exists(pickle_path):
        return pd.read_pickle(pickle_path)

    df = pd.DataFrame(columns=[
        "md5",
        "filename_in_prod",
        "filename_in_raw",
        "historical_prod",
        "historical_raw",
        "mtime",
        "size",
        "last_seen",
    ])
    df.set_index("md5", inplace=True)
    return df

def load_registry(pickle_path: str) -> pd.DataFrame:
    if os.path.exists(pickle_path):
        return pd.read_pickle(pickle_path)

    df = pd.DataFrame(columns=[
        "md5",
        "filename_in_prod",
        "filename_in_raw",
        "historical_prod",
        "historical_raw",
        "file_metadata",   # NEW
    ])
    df.set_index("md5", inplace=True)
    return df


def save_registry(df: pd.DataFrame, pickle_path: str) -> None:
    """Safely save registry to pickle using temp file + rename."""
    tmp_path = pickle_path + ".tmp"
    df.to_pickle(tmp_path)
    shutil.move(tmp_path, pickle_path)


# ==============================
# File Hashing
# ==============================

def hash_file(file_path: str, chunk_size: int = 8192) -> str:
    """Return md5 hash of a file."""
    md5 = hashlib.md5()
    with open(file_path, "rb") as f:
        while chunk := f.read(chunk_size):
            md5.update(chunk)
    return md5.hexdigest()


# ==============================
# Core Scan Function
# ==============================

def scan_folder_v1(
    folder_path: str,
    df: pd.DataFrame,
    mode: str,
    pickle_path: str,
    save_every_n: int = 500,
    save_every_sec: int = 300,
) -> pd.DataFrame:
    """
    Scan a folder (recursive), updating registry DataFrame.
    mode: "prod" or "raw"
    """
    assert mode in {"prod", "raw"}
    active_col = f"filename_in_{mode}"
    historical_col = f"historical_{mode}"

    file_paths = []
    for root, _, files in os.walk(folder_path):
        for fname in files:
            file_paths.append(os.path.join(root, fname))

    n_files = len(file_paths)
    existing_paths: Set[str] = set()

    # Counters
    skipped, rehashed, new = 0, 0, 0

    last_save_time = time.time()

    for i, path in enumerate(tqdm(file_paths, desc=f"Scanning {mode}", unit="file")):
        try:
            stat = os.stat(path)
            size, mtime = stat.st_size, stat.st_mtime
            existing_paths.add(path)

            # Check if this file already in registry
            found = False
            if not df.empty:
                # locate any rows containing this path
                mask = df[active_col].apply(lambda s: path in s if isinstance(s, set) else False)
                if mask.any():
                    found = True
                    md5 = df[mask].index[0]
                    row = df.loc[md5]

                    if row["size"] == size and row["mtime"] == mtime:
                        # unchanged
                        skipped += 1
                    else:
                        # modified
                        md5 = hash_file(path)
                        rehashed += 1
                        df = update_registry(df, md5, path, mode, size, mtime)
                else:
                    # not previously tracked
                    md5 = hash_file(path)
                    new += 1
                    df = update_registry(df, md5, path, mode, size, mtime)

            if not found and df.empty:
                # brand new registry
                md5 = hash_file(path)
                new += 1
                df = update_registry(df, md5, path, mode, size, mtime)

            # Periodic save
            if (i + 1) % save_every_n == 0 or (time.time() - last_save_time) > save_every_sec:
                save_registry(df, pickle_path)
                last_save_time = time.time()

        except Exception as e:
            print(f"⚠️ Error processing {path}: {e}")

    # Reconcile deletions
    df = reconcile_missing(df, existing_paths, mode)

    # Final save
    save_registry(df, pickle_path)

    # Summary
    print(f"\n=== Scan Summary ({mode}) ===")
    print(f"Total files seen: {n_files}")
    print(f"Skipped (unchanged): {skipped}")
    print(f"Rehashed (modified): {rehashed}")
    print(f"New files: {new}")
    print(f"Missing files moved to history: handled by reconcile_missing()")
    print("============================\n")

    return df

from pathlib import Path

from pathlib import Path

def scan_folder(
    folder_path: str,
    df: pd.DataFrame,
    mode: str,
    pickle_path: str,
    save_every_n: int = 500,
    save_every_sec: int = 300,
) -> pd.DataFrame:
    """
    Scan a folder (recursive), updating registry DataFrame.
    mode: "prod" or "raw"
    """
    assert mode in {"prod", "raw"}
    active_col = f"filename_in_{mode}"
    historical_col = f"historical_{mode}"

    # Gather all files in folder (recursive) as relative paths
    file_paths = []
    root_path = Path(folder_path)
    for root, _, files in os.walk(folder_path):
        for fname in files:
            full_path = Path(root) / fname
            rel_path = full_path.relative_to(root_path).as_posix()  # << patched here
            file_paths.append(rel_path)

    n_files = len(file_paths)
    existing_paths: Set[str] = set()

    # Counters
    skipped, rehashed, new = 0, 0, 0

    last_save_time = time.time()

    for i, rel_path in enumerate(tqdm(file_paths, desc=f"Scanning {mode}", unit="file")):
        try:
            full_path = Path(folder_path) / rel_path
            stat = os.stat(full_path)
            size, mtime = stat.st_size, stat.st_mtime
            existing_paths.add(rel_path)

            # --- Check if this file already exists in registry ---
            found = False
            if not df.empty:
                # look for this relative path in any file_metadata dict
                candidate_rows = df[df["file_metadata"].apply(
                    lambda meta: isinstance(meta, dict) and rel_path in meta
                )]

                if not candidate_rows.empty:
                    found = True
                    md5 = candidate_rows.index[0]
                    row = df.loc[md5]
                    meta = row["file_metadata"]

                    stored_size, stored_mtime, _ = meta[rel_path]

                    if stored_size == size and stored_mtime == mtime:
                        skipped += 1
                        continue
                    else:
                        # file changed → rehash
                        md5 = hash_file(str(full_path))
                        rehashed += 1
                        df = update_registry(df, md5, rel_path, mode, size, mtime)
                else:
                    # not seen before → new file
                    md5 = hash_file(str(full_path))
                    new += 1
                    df = update_registry(df, md5, rel_path, mode, size, mtime)

            if not found and df.empty:
                # brand new registry
                md5 = hash_file(str(full_path))
                new += 1
                df = update_registry(df, md5, rel_path, mode, size, mtime)

            # --- Periodic checkpoint save ---
            if (i + 1) % save_every_n == 0 or (time.time() - last_save_time) > save_every_sec:
                save_registry(df, pickle_path)
                last_save_time = time.time()

        except Exception as e:
            print(f"⚠️ Error processing {rel_path}: {e}")

    # Reconcile deletions (works with relative paths)
    df = reconcile_missing(df, existing_paths, mode)

    # Final save
    save_registry(df, pickle_path)

    # Summary
    print(f"\n=== Scan Summary ({mode}) ===")
    print(f"Total files seen: {n_files}")
    print(f"Skipped (unchanged): {skipped}")
    print(f"Rehashed (modified): {rehashed}")
    print(f"New files: {new}")
    print(f"Missing files moved to history: handled by reconcile_missing()")
    print("============================\n")

    return df




def update_registry_v1(
    df: pd.DataFrame, md5: str, path: str, mode: str, size: int, mtime: float
) -> pd.DataFrame:
    """Insert or update a registry row for a given file."""
    active_col = f"filename_in_{mode}"

    if md5 not in df.index:
        df.loc[md5] = [
            set() if mode == "raw" else set(),
            set() if mode == "prod" else set(),
            set(),
            set(),
            mtime,
            size,
            datetime.now(timezone.utc),
        ]

    # Add to active set
    s: Set[str] = df.at[md5, active_col]
    if not isinstance(s, set):
        s = set()
    s.add(path)
    df.at[md5, active_col] = s

    # Update metadata
    df.at[md5, "size"] = size
    df.at[md5, "mtime"] = mtime
    df.at[md5, "last_seen"] = datetime.now(timezone.utc)

    return df

def update_registry(df, md5, path, mode, size, mtime):
    active_col = f"filename_in_{mode}"

    if md5 not in df.index:
        df.loc[md5] = [
            set() if mode == "raw" else set(),
            set() if mode == "prod" else set(),
            set(),
            set(),
            {},   # file_metadata
        ]

    # add to active set
    s = df.at[md5, active_col]
    if not isinstance(s, set):
        s = set()
    s.add(path)
    df.at[md5, active_col] = s

    # update file_metadata
    meta = df.at[md5, "file_metadata"]
    if not isinstance(meta, dict):
        meta = {}
    meta[path] = (size, mtime, datetime.now(timezone.utc))
    df.at[md5, "file_metadata"] = meta

    return df


def reconcile_missing_v1(df: pd.DataFrame, existing_paths: Set[str], mode: str) -> pd.DataFrame:
    """Move missing files from active set to historical set with timestamp."""
    active_col = f"filename_in_{mode}"
    historical_col = f"historical_{mode}"
    now = datetime.now(timezone.utc)

    for md5, row in df.iterrows():
        active: Set[str] = row[active_col] if isinstance(row[active_col], set) else set()
        hist: Set[Tuple[str, datetime]] = row[historical_col] if isinstance(row[historical_col], set) else set()

        removed = {p for p in active if p not in existing_paths}
        if removed:
            for r in removed:
                hist.add((r, now))
            active -= removed

        df.at[md5, active_col] = active
        df.at[md5, historical_col] = hist

    return df

def reconcile_missing(df, existing_paths, mode):
    active_col = f"filename_in_{mode}"
    historical_col = f"historical_{mode}"
    now = datetime.now(timezone.utc)

    for md5, row in df.iterrows():
        active = row[active_col] if isinstance(row[active_col], set) else set()
        hist = row[historical_col] if isinstance(row[historical_col], set) else set()
        meta = row["file_metadata"] if isinstance(row["file_metadata"], dict) else {}

        removed = {p for p in active if p not in existing_paths}
        if removed:
            for r in removed:
                hist.add((r, now))
                meta.pop(r, None)  # drop metadata for missing file
            active -= removed

        df.at[md5, active_col] = active
        df.at[md5, historical_col] = hist
        df.at[md5, "file_metadata"] = meta

    return df
# ==============================
# Convenience Wrappers
# ==============================

def scan_raw(df: pd.DataFrame, pickle_path: str, raw_path: str) -> pd.DataFrame:
    return scan_folder(raw_path, df, "raw", pickle_path)


def scan_prod(df: pd.DataFrame, pickle_path: str, prod_path: str) -> pd.DataFrame:
    return scan_folder(prod_path, df, "prod", pickle_path)


# ==============================
# Example Usage
# ==============================
if __name__ == "__main__":
    import settings  # you maintain this file with RAW_PATH, PROD_PATH, PICKLE_PATH

    #df = load_registry(settings.PICKLE_PATH)

    

    pickle_path = "pickle.pkl"
    raw_to_scan = r"F:\TF1\Pandryn\Raw"
    prod_to_scan = r"D:\Pandryn\Pandryn_Box"
    

    df = load_registry(pickle_path)    
    df = scan_prod(df, pickle_path, prod_to_scan)
    df = scan_raw(df, pickle_path, raw_to_scan)
