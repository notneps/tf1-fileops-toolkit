import os
import hashlib
import pandas as pd
from tqdm import tqdm
from pathlib import Path

def file_md5(filepath, chunk_size=8192):
    """Compute MD5 hash for a file."""
    md5 = hashlib.md5()
    with open(filepath, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            md5.update(chunk)
    return md5.hexdigest()

def hash_directory(root_dir, csv_out=None):
    all_files = []
    for base, _, files in os.walk(root_dir):
        for fname in files:
            all_files.append(os.path.join(base, fname))

    results = []
    #for fpath in tqdm(all_files, desc=f"Hashing {root_dir}"):
    for fpath in tqdm(all_files, desc=f"Hashing {Path(root_dir).name}"):
        try:
            h = file_md5(fpath)
            results.append({"filepath": fpath, "md5": h})
        except Exception as e:
            print(f"⚠️ Failed hashing {fpath}: {e}")
            results.append({"filepath": fpath, "md5": None})

    df = pd.DataFrame(results)
    if csv_out:
        df.to_csv(csv_out, index=False)
    return df


def compare_hashes(df_question, df_origin, csv_out=None):
    """
    Compare two hash DataFrames:
    - df_question: files to check (filepath, md5)
    - df_origin: reference files (filepath, md5)

    Returns DataFrame with [file_in_question, origin_match, md5].
    """
    # Build lookup of hash → origin filepath
    origin_lookup = (
        df_origin.dropna(subset=["md5"])
        .groupby("md5")["filepath"]
        .first()
        .to_dict()
    )

    results = []
    for _, row in tqdm(df_question.iterrows(), total=len(df_question), desc="Comparing"):
        h = row["md5"]
        match = origin_lookup.get(h, None)
        results.append({
            "file_in_question": row["filepath"],
            "origin_match": match,
            "md5": h
        })

    df = pd.DataFrame(results)
    if csv_out:
        df.to_csv(csv_out, index=False)
    return df
