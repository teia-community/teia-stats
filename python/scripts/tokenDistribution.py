import pandas as pd
import matplotlib.pyplot as plt

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

# Remove restricted users and drop the restricted column after that
users = users[users["restricted"] == False]
users = users.drop("restricted", axis=1)

# Print a summary of the users data
users.info()

# Check how many users we have of each type
users["type"].value_counts()

# Define the total number of TEIA tokens to distribute
total_amount = 1e6

# Calculate the TEIA token scaling factor based on the users registration info
sf = 1 + 1 * users["has_profile"] + 0.5 * users["verified"]
users["scaling_factor"] = sf

# Calculate the TEIA tokens that users will get based on their activity
amount = sf * users["active_days"]
users["activity_amount"] = 0.35 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get because they participated in
# the last Teia votes
amount = sf * users["teia_votes"].pow(0.5)
users["voting_amount"] = 0.1 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their minted OBJKTs
amount = sf * users["minted_objkts"].pow(0.5)
users["minting_amount"] = 0.1 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their collected OBJKTs
amount = sf * users["collected_objkts"].pow(0.5)
users["collecting_amount"] = 0.1 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their connections
amount = sf * users["connections_to_users"].pow(0.5)
users["connections_amount"] = 0.1 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that artists will get based on their earnings
amount = sf * users["money_earned_own_objkts"].pow(0.5)
users["earnings_amount"] = 0.1 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that collectors will get based on their spending
amount = sf * users["money_spent"].pow(0.5)
users["spending_amount"] = 0.1 * total_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their hDAO
amount = users["hdao"]
users["hdao_amount"] = 0.05 * total_amount * amount / amount.sum()

# Combine all the TEIA token amounts
users["total_amount"] = (
    users["activity_amount"] + 
    users["voting_amount"] + 
    users["minting_amount"] + 
    users["collecting_amount"] + 
    users["connections_amount"] + 
    users["earnings_amount"] + 
    users["spending_amount"] + 
    users["hdao_amount"])

# Order the users data by the total amount of TEIA tokens that they will receive
users = users.sort_values(by="total_amount", ascending=False)

# Print the top 50 TEIA token owners
columns = [
    "username", "type", "verified", "hdao", "activity_amount", "voting_amount",
    "minting_amount", "collecting_amount", "connections_amount",
    "earnings_amount", "spending_amount", "hdao_amount", "total_amount"]
users[columns][:50]

# Plot a histogram of the total amount of TEIA tokens per each user
cond = (users["total_amount"] > 5) & (users["total_amount"] < 400)
users[cond].hist("total_amount", bins=100)
plt.show()

# Save the data into a csv file
columns_to_save = [
    "username", "type", "verified", "has_profile", "hdao", "first_activity",
       "last_activity", "active_days", "minted_objkts", "collected_objkts",
       "swapped_objkts", "money_earned_own_objkts", "money_earned_other_objkts",
       "money_earned", "money_spent", "collaborations",
       "connections_to_artists", "connections_to_collectors",
       "connections_to_users", "teia_votes", "scaling_factor",
       "activity_amount", "voting_amount", "minting_amount",
       "collecting_amount", "connections_amount", "earnings_amount",
       "spending_amount", "hdao_amount", "total_amount"]
users[columns_to_save].to_csv("../data/token_distribution.csv")
