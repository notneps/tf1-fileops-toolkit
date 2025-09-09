from functions.file_comparison import hash_directory, compare_hashes
import pandas as pd



# Step 1: Hash both dirs once
#df_origin = hash_directory(r"F:\TF1\Pandryn\Raw\JA", csv_out="origin_hashes.csv")
df_question = hash_directory(r"D:\Pandryn\Pandryn_Box\JA", csv_out="question_hashes.csv")



# Step 2: Later, load from CSVs (no rehashing needed)
df_origin = pd.read_csv("origin_hashes.csv")
df_question = pd.read_csv("question_hashes.csv")

# Step 3: Compare
df_compare = compare_hashes(df_question, df_origin, csv_out="compare_results.csv")




#dir_in_question=r"C:\Users\nephi\Box\FSL + WALLY - Pandryn\JA",
#origin_dir=r"F:\TF1\Pandryn\Raw\JA",



#if True:
#    df_origin = hash_directory(r"F:/TF1/Pandryn/Raw/JA", csv_out="origin_hashes.csv")