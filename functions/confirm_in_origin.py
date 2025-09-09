import os
import hashlib
import pandas as pd
from tqdm import tqdm

def file_md5(filepath, chunk_size=8192):
    """Compute MD5 hash for a file."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()

def get_hashes_from_dir(root_dir, desc="Hashing files"):
    """Return a dict of {hash: [filepaths]} for all files under root_dir."""
    # Collect all file paths first
    all_files = []
    for base, _, files in os.walk(root_dir):
        for fname in files:
            all_files.append(os.path.join(base, fname))

    hash_map = {}
    for fpath in tqdm(all_files, desc=desc):
        try:
            h = file_md5(fpath)
            hash_map.setdefault(h, []).append(fpath)
        except Exception as e:
            print(f"⚠️ Failed hashing {fpath}: {e}")
    return hash_map

def confirm_in_origin(dir_in_question, origin_dir, csv_out):
    """Compare files in dir_in_question against origin_dir by MD5 and save results to CSV."""
    # Hash origin directory
    origin_hashes = get_hashes_from_dir(origin_dir, desc="Hashing origin_dir")
    
    # Hash files in question directory
    question_hashes = {}
    all_files_question = []
    for base, _, files in os.walk(dir_in_question):
        for fname in files:
            all_files_question.append(os.path.join(base, fname))
    
    results = []
    for fpath in tqdm(all_files_question, desc="Processing dir_in_question"):
        try:
            h = file_md5(fpath)
            match = origin_hashes.get(h, [None])[0]  # pick first match or None
            results.append({
                "file_in_question": fpath,
                "origin_match": match,
                "md5": h
            })
        except Exception as e:
            print(f"⚠️ Failed hashing {fpath}: {e}")
            results.append({
                "file_in_question": fpath,
                "origin_match": None,
                "md5": None
            })
    
    df = pd.DataFrame(results)
    df.to_csv(csv_out, index=False)
    return df
