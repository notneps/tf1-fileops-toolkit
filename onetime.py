import os
import pickle
import shutil
from pathlib import Path
import pandas as pd

# --- configure your paths here ---
#from settings import RAW_PATH, PROD_PATH, PICKLE_PATH
RAW_PATH = r"F:\TF1\Pandryn\Raw"
PROD_PATH = r"D:\Pandryn\Pandryn_Box"
PICKLE_PATH = "pickle.pkl"

def normalize_paths_in_set(paths, base_path):
    """Convert absolute paths to relative paths against base_path."""
    normalized = set()
    if not isinstance(paths, set):
        return normalized

    base = Path(base_path)
    for p in paths:
        try:
            rel = Path(p).relative_to(base)
            normalized.add(str(rel))  # native separators (\ on Windows)
        except Exception:
            # if it doesn’t belong to this base_path, leave it untouched
            normalized.add(p)
    return normalized


def normalize_paths_in_hist(hist_set, base_path):
    """Same as above, but for historical sets of tuples (path, timestamp)."""
    normalized = set()
    if not isinstance(hist_set, set):
        return normalized

    base = Path(base_path)
    for item in hist_set:
        try:
            path, ts = item
            rel = Path(path).relative_to(base)
            normalized.add((str(rel), ts))
        except Exception:
            normalized.add(item)
    return normalized


def normalize_paths_in_metadata(meta, base_path):
    """Fix file_metadata dict: {path: (size, mtime, last_seen)}."""
    normalized = {}
    if not isinstance(meta, dict):
        return normalized

    base = Path(base_path)
    for p, val in meta.items():
        try:
            rel = Path(p).relative_to(base)
            normalized[str(rel)] = val
        except Exception:
            normalized[p] = val
    return normalized


def clean_registry(df: pd.DataFrame) -> pd.DataFrame:
    """Rewrite path strings to relative ones."""
    for md5, row in df.iterrows():
        df.at[md5, "filename_in_prod"] = normalize_paths_in_set(row["filename_in_prod"], PROD_PATH)
        df.at[md5, "filename_in_raw"] = normalize_paths_in_set(row["filename_in_raw"], RAW_PATH)
        df.at[md5, "historical_prod"] = normalize_paths_in_hist(row["historical_prod"], PROD_PATH)
        df.at[md5, "historical_raw"] = normalize_paths_in_hist(row["historical_raw"], RAW_PATH)
        df.at[md5, "file_metadata"] = normalize_paths_in_metadata(row["file_metadata"], PROD_PATH) | \
                                      normalize_paths_in_metadata(row["file_metadata"], RAW_PATH)
    return df


if __name__ == "__main__":
    # backup original pickle
    backup_path = PICKLE_PATH + ".bak"
    shutil.copy2(PICKLE_PATH, backup_path)
    print(f"📂 Backup created: {backup_path}")

    # load, clean, save
    df = pd.read_pickle(PICKLE_PATH)
    df = clean_registry(df)

    tmp_path = PICKLE_PATH + ".tmp"
    df.to_pickle(tmp_path)
    shutil.move(tmp_path, PICKLE_PATH)

    print(f"✅ Registry cleaned and saved back to {PICKLE_PATH}")
