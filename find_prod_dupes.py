import pandas as pd

def find_duplicate_prod_files(pickle_path: str, csv_path: str) -> pd.DataFrame:
    """
    Load registry, filter for hashes with >1 Production file,
    add a prod_count column,
    return filtered df and save to CSV.
    """
    df = pd.read_pickle(pickle_path)

    # Count prod files per hash
    df["prod_count"] = df["filename_in_prod"].apply(
        lambda s: len(s) if isinstance(s, set) else 0
    )

    # Filter for duplicates
    dupes_df = df[df["prod_count"] > 1].copy()

    # Convert sets to readable strings for CSV
    for col in ["filename_in_prod", "filename_in_raw", "historical_prod", "historical_raw"]:
        if col in dupes_df.columns:
            dupes_df[col] = dupes_df[col].apply(
                lambda x: "; ".join(sorted(map(str, x))) if isinstance(x, (set, list)) else x
            )

    dupes_df.to_csv(csv_path, index=True)  # index is md5
    print(f"✅ Duplicate production files saved to {csv_path}")

    return dupes_df

from settings import PICKLE_PATH

dupes = find_duplicate_prod_files(PICKLE_PATH, "prod_duplicates.csv")
print(dupes.head())