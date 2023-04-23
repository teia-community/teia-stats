import pandas as pd

from teiaUtils.analysisUtils import save_json_file

# Download the hDAO snapshot information from google docs
hdao_snapshot_level = "3400856"
key = "1-a59_41HG_ia1NAsqKJPoENfyv2sJeOzvACTlPn2hWI"
gid = "2015593363"
url = "https://docs.google.com/spreadsheet/ccc?key=%s&gid=%s&output=csv" % (key, gid)
hdao_snapshot = pd.read_csv(url, index_col="Address")

# Multiply the balance by the decimals
hdao_snapshot["SUM"] = (1e6 * hdao_snapshot["SUM"]).astype(int)

# Remove rows with zero balance
hdao_snapshot = hdao_snapshot[hdao_snapshot.SUM > 0]

# Sort the users by their hDAO balance
hdao_snapshot = hdao_snapshot.sort_values(by="SUM", ascending=False)

# Transform the data frame into a python dictionary
hdao_snapshot = hdao_snapshot.to_dict()["SUM"]

# Save the data as a json file
file_name = "../data/hdao_snapshot_%s.json" % hdao_snapshot_level
save_json_file(file_name, hdao_snapshot)
