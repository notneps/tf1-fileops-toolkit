from md5_manager import scan_raw, scan_prod, scan_folder
import pandas as pd

pickle_path = "pickle.pkl"
raw_path = r"F:\TF1\Pandryn\Raw\Sao Daily Dump"


dataframe = pd.DataFrame
scan_raw(raw_path=raw_path, pickle_path=pickle_path, df=dataframe)