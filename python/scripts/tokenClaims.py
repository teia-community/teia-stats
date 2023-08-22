import numpy as np
import pandas as pd

from teiaUtils.queryUtils import *
from teiaUtils.analysisUtils import read_json_file, save_json_file

# Set the path to the directory where the transactions information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Get the complete list of TEIA token claim transactions
claims = get_all_transactions("claim", transactions_dir, sleep_time=2)

# Read the csv file with the token distribution information
date_columns = ["first_activity", "last_activity"]
token_distribution = pd.read_csv(
    "../data/token_distribution.csv", parse_dates=date_columns, keep_default_na=False)

# Set the user address as the index
token_distribution = token_distribution.set_index("address")

# Change the type and contribution type columns data type to categorical
token_distribution["type"] = pd.Categorical(token_distribution["type"])

# Print a summary of the token distribution data
token_distribution.info()

# Add a column with the claim information
token_distribution["claimed"] = False
claim_addresses = [claim["sender"]["address"] for claim in claims]
token_distribution.loc[claim_addresses, "claimed"] = True

# Get the TEIA token ledger information
ledger = get_token_bigmap(name="ledger", token="TEIA", data_dir=transactions_dir)

