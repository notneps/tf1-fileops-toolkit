import os
import hashlib
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def find_duplicates(folder_path, max_workers=8):
    """
    Multithreaded duplicate finder with progress bars.
    
    1. Scans all files (recursively)
    2. Computes MD5 hashes in parallel
    3. Creates two CSV reports:
        - all_files.csv: All files with their hashes
        - duplicates.csv: Only hashes that appear more than once
    """

    def md5_hash(file_path, chunk_size=8192):
        """Compute MD5 hash of a file efficiently."""
        hasher = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(chunk_size), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            tqdm.write(f"⚠️ Error hashing {file_path}: {e}")
            return None

    # Step 1: Gather all file paths
    all_files = []
    for root, _, files in os.walk(folder_path):
        for file in files:
            all_files.append(os.path.join(root, file))

    tqdm.write(f"Found {len(all_files)} files to scan.")

    # Step 2: Compute hashes in parallel with progress bar
    hash_dict = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(md5_hash, path): path for path in all_files}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Hashing files", unit="file"):
            file_path = futures[future]
            file_hash = future.result()
            if file_hash:
                hash_dict.setdefault(file_hash, []).append(file_path)

    # Step 3: Write all files CSV
    all_files_csv = os.path.join(folder_path, 'all_files.csv')
    with open(all_files_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['File Path', 'MD5 Hash'])
        for h, paths in hash_dict.items():
            for path in paths:
                writer.writerow([path, h])

    # Step 4: Write duplicates CSV
    duplicates_csv = os.path.join(folder_path, 'duplicates.csv')
    with open(duplicates_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['MD5 Hash', 'Duplicate File Paths'])
        for h, paths in hash_dict.items():
            if len(paths) > 1:
                writer.writerow([h, '; '.join(paths)])

    tqdm.write(f"✅ Scan complete.")
    tqdm.write(f"All files report: {all_files_csv}")
    tqdm.write(f"Duplicates report: {duplicates_csv}")



path = r"C:\Users\nephi\Box\FSL + WALLY - Pandryn\ZH_TW\PT Uploads"
path2 = r"C:\Users\nephi\Box\FSL + WALLY - Pandryn\ZH_TW\BP Uploads"
find_duplicates(path2)