import json
from urllib.request import urlopen, Request

def get_query_result(query, timeout=10):
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3"}

    with urlopen(Request(query, headers=headers), timeout=timeout) as request:
        if request.status == 200:
            return json.loads(request.read().decode())

# Get the list of users allowed to vote
teia_users = get_query_result(
    "https://cache.teia.rocks/ipfs/QmNihShvZkXq7aoSSH3Nt1VeLjgGkESr3LoCzShNyV4uzp")

# Get the poll information from ipfs
poll_id = "QmeJ9ATjn4ge9phDzvpmdZzRZdRoKJdyk4swPiVgaxAx6z"
poll_information = get_query_result("https://infura-ipfs.io/ipfs/" + poll_id)

if poll_information["multi"] == "false":
    poll_information["opt1"] = "YES"
    poll_information["opt2"] = "NO"

# Get the votes associated to the poll
all_votes = get_query_result("https://api.mainnet.tzkt.io/v1/bigmaps/64367/keys?limit=10000&key.string=" + poll_id)

# Select only those votes that come from teia users wallets
valid_votes = [vote for vote in all_votes if vote["key"]["address"] in teia_users]
print("")
print("%4i teia users have voted so far." % len(valid_votes))
print("%4i votes were invalid because they didn't come from a wallet in the teia users list." % (len(all_votes) - len(valid_votes)))

# Initialize the results dictionary taking the names from the poll information
results = {str(i): {"name": poll_information["opt" + str(i)], "votes": 0} for i in range(1, 4)}

# Count the valid votes
for vote in valid_votes:
    results[vote["value"]]["votes"] += 1

# Print the results
print("")
print("Preliminary results:")
print("")

for entry in results.values():
    print("%10s: %4i votes (%4.1f%%)" % (entry["name"], entry["votes"], entry["votes"] * 100 / len(valid_votes)))

# Verify your vote
your_wallet = "tz1g6JRCpsEnD2BLiAzPNK3GBD1fKicV9rCx"  ## You need to edit this with your address
your_vote = None

for vote in valid_votes:
    if vote["key"]["address"] == your_wallet:
        your_vote = results[vote["value"]]["name"]

print("")
print("You didn't vote" if your_vote is None else "You voted for %s" % your_vote)
print("")
