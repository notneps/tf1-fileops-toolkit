import os
import hashlib
import csv

def find_duplicates(folder_path):
    """
    Scans all files in a folder (recursively), computes MD5 hashes,
    identifies duplicates, and exports two CSV reports:
        1. all_files.csv — all files and their hashes
        2. duplicates.csv — only hashes that appear more than once
    """

    def md5_hash(file_path, chunk_size=8192):
        """Compute MD5 hash of a file without loading it all into memory."""
        hasher = hashlib.md5()
        try:
            with open(file_path, 'rb') as f:
                for chunk in iter(lambda: f.read(chunk_size), b''):
                    hasher.update(chunk)
            return hasher.hexdigest()
        except Exception as e:
            print(f"Error hashing {file_path}: {e}")
            return None

    # Step 1: Walk folder and collect all files
    hash_dict = {}  # hash -> list of file paths
    for root, _, files in os.walk(folder_path):
        for file in files:
            file_path = os.path.join(root, file)
            file_hash = md5_hash(file_path)
            if file_hash:
                hash_dict.setdefault(file_hash, []).append(file_path)

    # Step 2: Write all files and hashes
    all_files_csv = os.path.join(folder_path, 'all_files.csv')
    with open(all_files_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['File Path', 'MD5 Hash'])
        for h, paths in hash_dict.items():
            for path in paths:
                writer.writerow([path, h])

    # Step 3: Write duplicates only
    duplicates_csv = os.path.join(folder_path, 'duplicates.csv')
    with open(duplicates_csv, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['MD5 Hash', 'Duplicate File Paths'])
        for h, paths in hash_dict.items():
            if len(paths) > 1:
                writer.writerow([h, '; '.join(paths)])

    print(f"✅ Scan complete.\nAll files report: {all_files_csv}\nDuplicates report: {duplicates_csv}")


path = r"C:\Users\nephi\Box\FSL + WALLY - Pandryn\ZH_TW\PT Uploads"
find_duplicates(path)