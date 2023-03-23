import pandas as pd
import matplotlib.pyplot as plt

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

# Remove restricted users and drop the restricted column after that
users = users[users["restricted"] == False]
users = users.drop("restricted", axis=1)

# Print a summary of the users data
users.info()

# Check how many users we have of each type
users["type"].value_counts()

# Define the total number of TEIA tokens to distribute between users based on
# their activity
total_activity_amount = 1.5e6

# Calculate the TEIA token scaling factor based on the users registration info
# and the days they have been active at the site
sf = (1 + 0.5 * users["has_profile"] + 0.5 * users["verified"])
sf[users["active_days"] < 7] = 0
sf[users["collaboration_level"] == 1] = 5
sf[users["collaboration_level"] == 2] = 8
users["scaling_factor"] = sf

# Calculate the TEIA tokens that users will get based on their activity
amount = sf * users["active_days"]
users["activity_amount"] = 0.18 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their Teia activity
amount = sf * users["teia_active_days"]
users["teia_activity_amount"] = 0.11 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get because they participated in
# the last Teia votes
amount = sf * users["teia_votes"].pow(0.5)
users["voting_amount"] = 0.14 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their minted OBJKTs
amount = sf * users["minted_objkts"].pow(0.5)
users["minting_amount"] = 0.08 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their collected OBJKTs
amount = sf * users["collected_objkts"] .pow(0.5)
users["collecting_amount"] = 0.11 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their connections
amount = sf * users["connections_to_users"].pow(0.5)
users["connections_amount"] = 0.18 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that artists will get based on their earnings
amount = sf * users["money_earned_own_objkts"].pow(0.5)
users["earnings_amount"] = 0.08 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that collectors will get based on their spending
amount = sf * users["money_spent"].pow(0.5)
users["spending_amount"] = 0.08 * total_activity_amount * amount / amount.sum()

# Calculate the TEIA tokens that users will get based on their collaboration level
amount = sf * users["collaboration_level"]
users["collaboration_amount"] = 0.04 * total_activity_amount * amount / amount.sum()

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
    users["collaboration_amount"])
users["total_amount"] = users["total_activity_amount"] + users["hdao_amount"]

# Order the users data by the total amount of TEIA tokens that they will receive
users = users.sort_values(by="total_amount", ascending=False)

# Print the top 50 TEIA token owners
columns = [
    "username", "type", "twitter", "verified", "hdao", "activity_amount",
    "teia_activity_amount", "voting_amount", "minting_amount",
    "collecting_amount", "connections_amount", "earnings_amount",
    "spending_amount", "collaboration_amount", "hdao_amount", "total_amount"]
users[columns][:50]

# Plot a histogram of the total amount of TEIA tokens per each user
cond = (users["total_amount"] > 0) & (users["total_amount"] < 400)
users[cond].hist("total_amount", bins=100)
plt.show()

# Save the data into a csv file
columns_to_save = [
    "username", "type", "twitter", "verified", "has_profile", "hdao",
    "collaboration_level", "first_activity", "last_activity", "active_days",
    "teia_active_days", "minted_objkts", "collected_objkts", "swapped_objkts",
    "money_earned_own_objkts", "money_earned_other_objkts", "money_earned",
    "money_spent", "collaborations", "connections_to_artists",
    "connections_to_collectors", "connections_to_users", "teia_votes",
    "scaling_factor", "activity_amount", "teia_activity_amount",
    "voting_amount", "minting_amount", "collecting_amount",
    "connections_amount", "earnings_amount", "spending_amount",
    "collaboration_amount", "hdao_amount", "total_amount"]
users[columns_to_save].to_csv("../data/token_distribution_15.csv")






"""
file_name = "/home/jgracia/drop.ts"
total = 0

with open(file_name, "w") as file:
    file.write("// Modify data according to your drop\n")
    file.write("// Data specification:\n")
    file.write("// Tezos address => Number of tokens to receive (including token decimals)\n")
    file.write("const data: { [key: string]: string } = {\n")

    # Loop over the users
    for wallet, total_amount in zip(users.index, users["total_amount"]):
        if int(total_amount * 1e6) > 0:
            file.write('   %s: "%i",\n' % (wallet, total_amount * 1e6))
            total += total_amount * 1e6

    file.write("};\n")
    file.write("\n")
    file.write("export default data;\n")

merkle_data = read_json_file(
    "/home/jgracia/github/token-drop-template/deploy/src/merkle_build/mrklData.json")
addresses = list(merkle_data.keys())

batch_size = 4000
counter = 0
start = 0
end = min(batch_size, len(addresses))
mapping = {}

while start < len(addresses):
    data_batch = {}

    for address in addresses[start:end]:
        mapping[address] = counter
        data_batch[address] = merkle_data[address]

    file_name = "/home/jgracia/merkle_data_%i_%i.json" % (start, end)
    save_json_file(file_name, data_batch, compact=True)
    start = end
    end = min(start + batch_size, len(addresses)) 
    counter += 1

save_json_file("/home/jgracia/mapping.json", mapping, compact=True)
"""
