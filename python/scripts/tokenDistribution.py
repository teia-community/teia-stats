import numpy as np
import pandas as pd

from teiaUtils.analysisUtils import read_json_file, save_json_file

# Read the csv file containing all the users data
date_columns = [
    "first_activity", "last_activity", "first_mint", "last_mint",
    "first_collect", "last_collect", "first_swap", "last_swap"]
users = pd.read_csv(
    "../data/teia_users.csv", parse_dates=date_columns, keep_default_na=False)

# Set the user address as the index
users = users.set_index("address")

# Change the type column data type to categorical
users["type"] = pd.Categorical(users["type"])

# Add a column to indicate if a user is a teia contributor or not
users["contributor"] = users["contribution_level"] > 0

# Remove restricted users and drop the restricted column after that
users = users[users["restricted"] == False]
users = users.drop("restricted", axis=1)

# Print a summary of the users data
users.info()

# Check how many users we have of each type
users["type"].value_counts()

# Calculate the TEIA token scaling factor based on the users registration info
sf = np.ones(len(users))
sf[users["has_profile"]] = 2
sf[users["verified"]] = 3

# Filter possible bots and very inactive users
sf[(users["active_days"] < 14) & (users["teia_votes"] == 0) & (users["money_spent"] < 1000)] = 0

# Make sure that contributors have the same factor as verified users
sf[users["contributor"]] = 3

# Save the scaling factor information in the data frame
users["scaling_factor"] = sf

# Define the total amount of tokens that will be distributed
total_amount = 8.0e6

# Define the amount of tokens reserved for the DAO treasury
treasury_amount = 3.5e5

# Define the amount of tokens to distribute between users based on their activity
total_activity_amount = total_amount - treasury_amount - users["hdao"].sum()

# Calculate the TEIA tokens that users will get based on their activity
amount = sf * users["active_days"]
users["activity_amount"] = 0.15 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their Teia activity
amount = sf * users["teia_active_days"]
users["teia_activity_amount"] = 0.15 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get because they participated in
# the last Teia votes
amount = sf * users["teia_votes"].pow(0.5)
users["voting_amount"] = 0.10 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their minted OBJKTs
amount = sf * users["minted_objkts"].pow(0.5)
users["minting_amount"] = 0.07 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their collected OBJKTs
amount = sf * users["collected_objkts"] .pow(0.5)
users["collecting_amount"] = 0.08 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their connections
amount = sf * users["connections_to_users"].pow(0.5)
users["connections_amount"] = 0.07 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that artists will get based on their earnings
amount = sf * (~users["wash_trader"]) * users["money_earned_own_objkts"].pow(0.5)
users["earnings_amount"] = 0.15 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that collectors will get based on their spending
amount = sf * (~users["wash_trader"]) * users["money_spent"].pow(0.5)
users["spending_amount"] = 0.15 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their contribution level
amount = sf * users["contribution_level"]
users["contribution_amount"] = 0.08 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their hDAO
amount = users["hdao"]
users["hdao_amount"] = amount

# Combine all the TEIA token amounts
users["total_activity_amount"] = (
    users["activity_amount"] + 
    users["teia_activity_amount"] + 
    users["voting_amount"] + 
    users["minting_amount"] + 
    users["collecting_amount"] + 
    users["connections_amount"] + 
    users["earnings_amount"] + 
    users["spending_amount"] + 
    users["contribution_amount"])
users["total_amount"] = users["total_activity_amount"] + users["hdao_amount"]

# Order the users data by the total amount of TEIA tokens that they will receive
users = users.sort_values(by="total_amount", ascending=False)

# Print the top 50 TEIA token owners
columns = [
    "username", "type", "contributor", "twitter", "verified", "hdao",
    "activity_amount", "teia_activity_amount", "voting_amount",
    "minting_amount", "collecting_amount", "connections_amount",
    "earnings_amount", "spending_amount", "contribution_amount", "hdao_amount",
    "total_amount"]
users[columns][:50]

# Save the data into a csv file
columns_to_save = [
    "username", "type", "contributor", "contribution_level", "twitter",
    "verified", "has_profile", "hdao", "first_activity", "last_activity",
    "active_days", "teia_active_days", "minted_objkts", "collected_objkts",
    "swapped_objkts", "money_earned_own_objkts", "money_earned_other_objkts",
    "money_earned", "money_spent", "collaborations", "connections_to_artists",
    "connections_to_collectors", "connections_to_users", "teia_votes",
    "scaling_factor", "activity_amount", "teia_activity_amount",
    "voting_amount", "minting_amount", "collecting_amount",
    "connections_amount", "earnings_amount", "spending_amount",
    "contribution_amount", "hdao_amount", "total_amount"]
users[columns_to_save].to_csv("../data/token_distribution_8M.csv")

# Save the data with a format that can be used to create a Merkle tree
with open("../data/merkle_tree_input_8M.ts", "w") as file:
    file.write("// Modify data according to your drop\n")
    file.write("// Data specification:\n")
    file.write("// Tezos address => Number of tokens to receive (including token decimals)\n")
    file.write("const data: { [key: string]: string } = {\n")

    for wallet, total_amount in zip(users.index, users["total_amount"]):
        if int(total_amount * 1e6) > 0:
            file.write("    %s: '%i',\n" % (wallet, total_amount * 1e6))

    file.write("};\n")
    file.write("\n")
    file.write("export default data;\n")

# Save a snapshot for the next teia voting
cond = (users["total_amount"] >= 1) | (users["teia_active_days"] >= 3)
users_to_vote = users[cond]
len(users_to_vote)
save_json_file("../data/teia_and_hen_users_snapshot_17-05-2023.json", users_to_vote.index.array.tolist())
