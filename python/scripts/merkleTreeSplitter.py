import os.path

from teiaUtils.analysisUtils import read_json_file, save_json_file

# Read the Merkle tree data associated to the token distribution
token_drop_template_directory = "/home/jgracia/github/token-drop-template"
merkle_data = read_json_file(os.path.join(token_drop_template_directory, "deploy/src/merkle_build/mrklData.json"))

# Extract the list of addresses
addresses = list(merkle_data.keys())

# Save the proofs in a list of json files
output_dir = "/home/jgracia"
batch_size = 3000
counter = 0
mapping = {}

while counter < (len(addresses) / batch_size):
    data_batch = {}
    start = counter * batch_size
    end = min(start + batch_size, len(addresses))

    for address in addresses[start:end]:
        mapping[address] = counter
        data_batch[address] = merkle_data[address]

    file_name = "merkle_data_%i.json" % counter
    save_json_file(os.path.join(output_dir, file_name), data_batch, compact=True)
    counter += 1

# Save the mapping between the addresses and the data files
save_json_file("/home/jgracia/mapping.json", mapping, compact=True)
