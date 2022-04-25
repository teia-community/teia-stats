import teiaUtils.analysisUtils as utis
from teiaUtils.queryUtils import *
from teiaUtils.plotUtils import *

# Set the path to the directory where the tezos wallets information will be
# saved to avoid to query for it again and again
wallets_dir = "../data/wallets"

# Set the path to the directory where the transactions information will be saved
# to avoid to query for it again and again
transactions_dir = "../data/transactions"

# Set the path to the directory where the figures will be saved
figures_dir = "../figures"

# Exclude the last day from most of the plots?
exclude_last_day = True

# Get the complete list of tezos wallets
wallets = get_tezos_wallets(wallets_dir, sleep_time=10)

# Get the complete list of H=N mint, collect, swap and cancel swap transactions
hen_mints = get_all_transactions("mint", transactions_dir, sleep_time=10)
hen_collects = get_all_transactions("hen_collect", transactions_dir, sleep_time=10)
hen_swaps = get_all_transactions("hen_swap", transactions_dir, sleep_time=10)
hen_cancel_swaps = get_all_transactions("hen_cancel_swap", transactions_dir, sleep_time=10)

# Get the complete list of Teia collect, swap and cancel swap transactions
teia_collects = get_all_transactions("teia_collect", transactions_dir, sleep_time=10)
teia_swaps = get_all_transactions("teia_swap", transactions_dir, sleep_time=10)
teia_cancel_swaps = get_all_transactions("teia_cancel_swap", transactions_dir, sleep_time=10)

# Get the H=N bigmaps
hen_swaps_bigmap = get_hen_bigmap("swaps", transactions_dir, sleep_time=10)
hen_royalties_bigmap = get_hen_bigmap("royalties", transactions_dir, sleep_time=10)
hen_registries_bigmap = get_hen_bigmap("registries", transactions_dir, sleep_time=10)
hen_subjkts_metadata_bigmap = get_hen_bigmap("subjkts metadata", transactions_dir, sleep_time=10)

# Get the Teia swaps bigmap
teia_swaps_bigmap = get_teia_bigmap("swaps", transactions_dir, sleep_time=1)

# Get users information from the mint, collect and swap transactions
users = {}
users = utils.add_mints_to_users(hen_mints, users)
users = utils.add_collects_to_users(hen_collects, hen_swaps_bigmap, users)
users = utils.add_collects_to_users(teia_collects, teia_swaps_bigmap, users)
users = utils.add_swaps_to_users(hen_swaps, users)
users = utils.add_swaps_to_users(teia_swaps, users)

# Add the restricted wallets information
restricted_wallets = get_restricted_wallets()
users = utils.add_restricted_wallets_information(restricted_wallets, users)

# Add the user names
users = utils.add_usernames(hen_registries_bigmap, {}, wallets, users)

# Separate between artists, collectors, patrons, swappers and restricted users
artists = {}
collectors = {}
patrons = {}
swappers = {}
restricted = {}

for wallet, user in users.items():
    if user.restricted:
        restricted[wallet] = user
    else:
        if user.type == "artist":
            artists[wallet] = user

            if len(user.collected_objkts) > 0:
                collectors[wallet] = user
        elif user.type == "patron":
            patrons[wallet] = user
            collectors[wallet] = user
        elif user.type == "swapper":
            swappers[wallet] = user

# Print some information about the total number of users
print("There are currently %i H=N and Teia users." % (
    len(users) - len(restricted)))
print("Of those %i are artists, %i are patrons and %i are swappers." % (
    len(artists), len(patrons), len(swappers)))
print("%i artists are also collectors." % (len(collectors) - len(patrons)))
print("%i users are in the restricted list." % len(restricted))

# Plot the number of operations per day
plot_operations_per_day(
    hen_mints, "Mint operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Mint operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_mints_per_day.png"))

plot_operations_per_day(
    hen_collects, "H=N collect operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Collect operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_collects_per_day.png"))

plot_operations_per_day(
    teia_collects, "Teia collect operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Collect operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_collects_per_day.png"))

plot_operations_per_day(
    hen_swaps, "H=N swap operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Swap operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_swaps_per_day.png"))

plot_operations_per_day(
    teia_swaps, "Teia swap operations per day",
    "Days since first minted OBJKT (1st of March)",
    "Swap operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_swaps_per_day.png"))

plot_operations_per_day(
    hen_cancel_swaps, "H=N cancel_swap operations per day",
    "Days since first minted OBJKT (1st of March)",
    "cancel_swap operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_cancel_swaps_per_day.png"))

plot_operations_per_day(
    teia_cancel_swaps, "Teia cancel_swap operations per day",
    "Days since first minted OBJKT (1st of March)",
    "cancel_swap operations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_cancel_swaps_per_day.png"))
