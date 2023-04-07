from teiaUtils.queryUtils import *
from teiaUtils.plotUtils import *
from teiaUtils.teiaUsers import TeiaUsers
from teiaUtils.analysisUtils import read_json_file, read_csv_file

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
wallets = get_tezos_wallets(wallets_dir, sleep_time=2)

# Get the complete list of tzprofiles
tzprofiles = get_tzprofiles(sleep_time=1)

# Get the tezos domains information
tezos_domains_owners = get_tezos_domains_owners(sleep_time=1)

# Get the fxhash user names
fxhash_usernames = get_fxhash_usernames(transactions_dir)

# Get the complete list of H=N mint, collect, swap and cancel swap transactions
hen_mints = get_all_transactions("mint", transactions_dir, sleep_time=2)
hen_mint_objkts = get_all_transactions("mint_OBJKT", transactions_dir, sleep_time=2)
hen_collects = get_all_transactions("hen_collect", transactions_dir, sleep_time=2)
hen_swaps = get_all_transactions("hen_swap", transactions_dir, sleep_time=2)
hen_cancel_swaps = get_all_transactions("hen_cancel_swap", transactions_dir, sleep_time=2)

# Get the complete list of Teia collect, swap and cancel swap transactions
teia_collects = get_all_transactions("teia_collect", transactions_dir, sleep_time=2)
teia_swaps = get_all_transactions("teia_swap", transactions_dir, sleep_time=2)
teia_cancel_swaps = get_all_transactions("teia_cancel_swap", transactions_dir, sleep_time=2)

# Get the H=N bigmaps
hen_swaps_bigmap = get_hen_bigmap("swaps", transactions_dir, sleep_time=2)
hen_royalties_bigmap = get_hen_bigmap("royalties", transactions_dir, sleep_time=2)
hen_registries_bigmap = get_hen_bigmap("registries", transactions_dir, sleep_time=2)
hen_subjkts_metadata_bigmap = get_hen_bigmap("subjkts metadata", transactions_dir, sleep_time=2)

# Get the Teia swaps bigmap
teia_swaps_bigmap = get_teia_bigmap("swaps", transactions_dir, sleep_time=10)

# Get the artists collaborations information
artists_collaborations = get_artists_collaborations()
artists_collaborations_signatures = get_artists_collaborations_signatures()

# Get the hDAO snapshot information
hdao_snapshot_level = 3263366
hdao_snapshot = read_json_file("../data/hdao_snapshot_%s.json" % hdao_snapshot_level)

# Get the users contribution levels
contribution_levels = read_csv_file("../data/teiaContributionLevels.csv")
contribution_levels = contribution_levels.set_index("address")
contribution_levels = contribution_levels.to_dict()["level"]

# Get the Teia users from the mint, collect and swap transactions
users = TeiaUsers()
users.add_mint_transactions(hen_mints, hen_mint_objkts)
users.add_collect_transactions(hen_collects, hen_swaps_bigmap, hen_royalties_bigmap)
users.add_collect_transactions(teia_collects, teia_swaps_bigmap, hen_royalties_bigmap)
users.add_swap_transactions(hen_swaps)
users.add_swap_transactions(teia_swaps)

# Include also the hDAO owners
users.add_hdao_information(hdao_snapshot, hdao_snapshot_level)

# Add the users contribution levels
users.add_contribution_level_information(contribution_levels)

# Add the restricted wallets information
restricted_addresses = get_restricted_addresses()
users.add_restricted_addresses_information(restricted_addresses)

# Add the wash trading addresses information
wash_trading_addresses = [
    "tz1eee5rapGDbq2bcZYTQwNbrkB4jVSQSSHx",  # hDAO wash trading
    "tz1bpz9S6JyBzMvJ97qPL7TeejkUiLjdkDAm",  # hDAO wash trading
    "tz2UuUoPpH51i6PTt9Bc7iBZ4ibhHUsczcwY",  # hDAO wash trading
    "tz1bhMc5uPJynkrHpw7pAiBt6YMhQktn7owF",  # hDAO wash trading
    "tz1VWBwFKLq6TCrPEVU8sZDfrcbx9buqMxnZ",  # hDAO wash trading
    "tz1e52CpddrybiDgawzvqRPXthy2hbAWGJba",  # hDAO wash trading
    "tz1Uby674S4xEw8w7iuM3GEkWZ3fHeHjT696",  # hDAO wash trading
    "tz1U3YJZ1pFfkaUWZVWv7FvNyiUi5vKYG696",  # hDAO wash trading
    "tz1Us9HFxfVZUZ5rn1Y9gYh3LkiS9nEDGmZS",  # hDAO wash trading
    "tz1f7zNRyNpBbNRx9xa4jW6XYgZdSYMH777n",  # Suspicious swaps/collects
    "tz1gSs2PFWtUXA5BjGzfXW2xTGaSQTvyZK2w",  # Suspicious swaps/collects
    "tz1biNdKY6ddqn1E7XMyCJLjJKNX8NMeZo4Z",  # Suspicious swaps/collects
    "tz1i7Uj8fFQ8vwdMkqGYTAhfYqPUGHxnVgTg",  # Suspicious swaps/collects
    "tz1aMnb63FDRwG5RYZG76HwrLinK7h9VT48H",  # Suspicious swaps/collects
    "tz1SUPNYXG7e1Zjn1WPuUFfEFmLJY7KrwPDw",  # Suspicious swaps/collects
    "tz1RHRH92Zt3ruxJWwUuu6C7FsrgoVzSCJZj",  # Suspicious swaps/collects
    "tz1ifgfKyPnptBAAumFFPKMcAV4gaRGTkfN8"]  # Suspicious swaps/collects
users.add_wash_trading_addresses_information(wash_trading_addresses)

# Add the profiles information
tzkt_metadata = get_users_tzkt_metadata(wallets_dir, users)
users.add_profiles_information(
    hen_registries_bigmap, tzprofiles, tzkt_metadata, tezos_domains_owners, fxhash_usernames)

# Add the Teia Community votes information
votes = get_teia_community_votes()
polls = ["QmU7zZepzHiLMUme1xRHZyTdbyD4j2EfUodiGJeA1Rv6QQ",
         "QmVSWZZcBT6zRrcZM6hf9VZJ7Qha5GXUBScQowJJ7fYQxT",
         "QmPDYWmGdxae8gUxqiPa4rkuQCc8P6sggLvUi5HQrrCzug",
         "QmQdgL954By1DNuam2abaQd4B8o9UzWaJgrfsK9xjabWQg"]
users.add_teia_community_votes(votes, polls)

# Add the artists collaborations information
users.add_artists_collaborations(artists_collaborations, artists_collaborations_signatures)

# Compress the user connections to save some memory
users.compress_user_connections()

# Select the artists, collectors, patrons, swappers and restricted users
artists = users.select("artists").select("not_restricted").select("not_contract")
collectors = users.select("collectors").select("not_restricted").select("not_contract")
patrons = users.select("patrons").select("not_restricted").select("not_contract")
swappers = users.select("swappers").select("not_restricted").select("not_contract")
hdao_owners = users.select("hdao_owners").select("not_restricted").select("not_contract")
collaborations = users.select("collaborations").select("not_restricted")
contracts = users.select("contract").select("not_restricted")
restricted = users.select("restricted")
wash_traders = users.select("wash_traders")

# Save as a csv file the complete list of users, excluding contracts
users.select("not_contract").save_as_csv_file("../data/teia_users.csv")

# Print some information about the total number of users
print("There are currently %i H=N and Teia users." % (
    len(users) - len(contracts) - len(restricted)))
print("Of those %i are artists, %i are patrons and %i are swappers." % (
    len(artists), len(patrons), len(swappers)))
print("%i artists are also collectors." % (len(collectors) - len(patrons)))
print("%i wallets are smart contracts." % len(contracts))
print("%i wallets are in the restricted list." % len(restricted))

# Print the top selling artists
print("\n This is the list of the top selling artists:\n")
top_100_selling_artists_addresses = users.get_top_selling_artists(100)

for i, address in enumerate(top_100_selling_artists_addresses):
    artist = users[address]

    if artist.username is not None:
        print(" %3i: Artist %s sold for %6.0f tez (%s)" % (
            i + 1, artist.address, artist.total_money_earned_own_objkts, artist.username))
    else:
        print(" %3i: Artist %s sold for %6.0f tez" % (
            i + 1, artist.address, artist.total_money_earned_own_objkts))

# Print the top collectors
print("\n This is the list of the top collectors:\n")
top_100_collectors_addresses = users.get_top_collectors(100)

for i, address in enumerate(top_100_collectors_addresses):
    collector = users[address]

    if collector.username is not None:
        print(" %3i: Collector %s spent %6.0f tez (%s)" % (
            i + 1, collector.address, collector.total_money_spent, collector.username))
    else:
        print(" %3i: Collector %s spent %6.0f tez" % (
            i + 1, collector.address, collector.total_money_spent))

# Plot the number of transactions per day
plot_transactions_per_day(
    hen_mints, "Mint transactions per day",
    "Days since first minted OBJKT (1st of March)",
    "Mint transactions per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_mints_per_day.png"))

plot_transactions_per_day(
    hen_collects, "H=N collect transactions per day",
    "Days since first minted OBJKT (1st of March)",
    "Collect transactions per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_collects_per_day.png"))

plot_transactions_per_day(
    teia_collects, "Teia collect transactions per day",
    "Days since 18th of March 2022",
    "Collect transactions per day", first_year=2022, first_month=3,
    first_day=18, exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_collects_per_day.png"))

plot_transactions_per_day(
    hen_swaps, "H=N swap transactions per day",
    "Days since first minted OBJKT (1st of March)",
    "Swap transactions per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_swaps_per_day.png"))

plot_transactions_per_day(
    teia_swaps, "Teia swap transactions per day",
    "Days since 18th of March 2022",
    "Swap transactions per day", first_year=2022, first_month=3,
    first_day=18, exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_swaps_per_day.png"))

plot_transactions_per_day(
    hen_cancel_swaps, "H=N cancel_swap transactions per day",
    "Days since first minted OBJKT (1st of March)",
    "cancel_swap transactions per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_cancel_swaps_per_day.png"))

plot_transactions_per_day(
    teia_cancel_swaps, "Teia cancel_swap transactions per day",
    "Days since 18th of March 2022",
    "cancel_swap transactions per day", first_year=2022, first_month=3,
    first_day=18, exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_cancel_swaps_per_day.png"))

# Plot the new users per day
plot_new_users_per_day(
    artists, title="New artists per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New artists per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_artists_per_day.png"))

plot_new_users_per_day(
    collectors, title="New collectors per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New collectors per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_collectors_per_day.png"))

plot_new_users_per_day(
    patrons, title="New patrons per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New patrons per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_patrons_per_day.png"))

plot_new_users_per_day(
    swappers, title="New swappers per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New swappers per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_swappers_per_day.png"))

plot_new_users_per_day(
    restricted, title="New restricted users per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New restricted users per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_restricted_users_per_day.png"))

plot_new_users_per_day(
    collaborations, title="New artists collaborations per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New artist collaborations per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_collabs_per_day.png"))

plot_new_users_per_day(
    users.select("not_restricted").select("not_hdao_owners"),
    title="New users per day",
    x_label="Days since first minted OBJKT (1st of March)",
    y_label="New users per day", exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "new_users_per_day.png"))

# Get the collected money that doesn't come from a restricted user
hen_collects_timestamps = []
hen_collects_money = []
teia_collects_timestamps = []
teia_collects_money = []

for collect in hen_collects:
    if collect["sender"]["address"] not in restricted_addresses:
        hen_collects_timestamps.append(collect["timestamp"])
        hen_collects_money.append(collect["amount"] / 1e6)

for collect in teia_collects:
    if collect["sender"]["address"] not in restricted_addresses:
        teia_collects_timestamps.append(collect["timestamp"])
        teia_collects_money.append(collect["amount"] / 1e6)

hen_collects_timestamps = np.array(hen_collects_timestamps)
hen_collects_money = np.array(hen_collects_money)
teia_collects_timestamps = np.array(teia_collects_timestamps)
teia_collects_money = np.array(teia_collects_money)

# Plot the money spent in collect operations per day
plot_data_per_day(
    hen_collects_money, hen_collects_timestamps,
    "Money spent in collect operations per day (H=N contract)",
    "Days since first minted OBJKT (1st of March)", "Money spent (tez)",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_money_per_day.png"))

plot_data_per_day(
    teia_collects_money, teia_collects_timestamps,
    "Money spent in collect operations per day (Teia contract)",
    "Days since 18th of March 2022", "Money spent (tez)", first_year=2022,
    first_month=3, first_day=18, exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_money_per_day.png"))

# Get the addresses and timestamps of each transaction
addresses = []
timestamps = []

for transaction in hen_mint_objkts:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

for transaction in hen_collects:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

for transaction in hen_swaps:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

for transaction in hen_cancel_swaps:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

addresses = np.array(addresses)
timestamps = np.array(timestamps)

# Plot the active users per day
plot_active_users_per_day(
    addresses, timestamps, users, "H=N active users per day",
    "Days since first minted OBJKT (1st of March)", "Active users per day",
    exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "hen_active_users_per_day.png"))

# Plot the active users per month
plot_active_users_per_month(
    addresses, timestamps, users, "H=N active users per month",
    "Months since first minted OBJKT (1st of March)", "Active users per month",
    exclude_last_month=False)
save_figure(os.path.join(figures_dir, "hen_active_users_per_month.png"))

# Get the addresses and timestamps of each transaction
addresses = []
timestamps = []

for transaction in teia_collects:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

for transaction in teia_swaps:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

for transaction in teia_cancel_swaps:
    addresses.append(transaction["sender"]["address"])
    timestamps.append(transaction["timestamp"])

addresses = np.array(addresses)
timestamps = np.array(timestamps)

# Plot the active users per day
plot_active_users_per_day(
    addresses, timestamps, users, "Teia active users per day",
    "Days since 18th of March 2022", "Active users per day", first_year=2022,
    first_month=3, first_day=18, exclude_last_day=exclude_last_day)
save_figure(os.path.join(figures_dir, "teia_active_users_per_day.png"))

# Plot the active users per month
plot_active_users_per_month(
    addresses, timestamps, users, "Teia active users per month",
    "Months since 18th of March 2022", "Active users per month", first_year=2022,
    first_month=3, exclude_last_month=False)
save_figure(os.path.join(figures_dir, "teia_active_users_per_month.png"))
