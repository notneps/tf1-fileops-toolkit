from pandasgui import show
import pickle as pickle 

pickle_file = "pickle.pkl"


# Open and load the pickle
with open(pickle_file, "rb") as f:
    data = pickle.load(f)

# If you want to view the dataframe in pandasgui:
#from pandasgui import show
#show(data)



data.to_csv("output.csv")

