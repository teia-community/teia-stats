import numpy as np
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

# Extract the artists accounts
artists = extract_artist_accounts(hen_mints, hen_registries_bigmap, wallets)

