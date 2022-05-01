import numpy as np

from teiaUtils.analysisUtils import get_datetime_from_timestamp


class TeiaUser:
    """This class collects all the information associated with a Teia user.

    """

    def __init__(self, address, id):
        """The class constructor.

        Parameters
        ----------
        address: str
            The user tz address.
        id: str
            The user id.

        """
        # General information
        self.address = address
        self.id = id
        self.type = None
        self.restricted = False

        # User names
        self.username = None
        self.tzkt_username = None
        self.tzprofiles_username = None
        self.hen_username = None

        # hDAO information
        self.hdao = 0
        self.hdao_snapshot_level = None

        # Activity information
        self.first_activity = None
        self.last_activity = None
        self.first_mint = None
        self.last_mint = None
        self.first_collect = None
        self.last_collect = None
        self.first_swap = None
        self.last_swap = None
        self.mint_timestamps = []
        self.collect_timestamps = []
        self.swap_timestamps = []

        # OBJKTs information
        self.minted_objkts = []
        self.collected_objkts = []
        self.swapped_objkts = []

        # Money related information
        self.money_earned_own_objkts = []
        self.money_earned_other_objkts = []
        self.money_spent = []
        self.total_money_earned_own_objkts = 0
        self.total_money_earned_collaborations_objkts = 0
        self.total_money_earned_other_objkts = 0
        self.total_money_earned = 0
        self.total_money_spent = 0

        # Connections with other users
        self.artist_connections = {}
        self.collector_connections = {}

        # Collaborations
        self.collaborations = []

        # Teia community votes
        self.teia_community_votes = {}

    def set_restricted(self, is_restricted):
        """Sets the user as restricted or not.

        Parameters
        ----------
        is_restricted: bool
            True if the user is in the Teia restricted users list.

        """
        self.restricted = is_restricted

    def set_hdao(self, hdao, level=None):
        """Sets the amount of hDAO owned by the user at a given block level.

        Parameters
        ----------
        hdao: int
            The number of hDAO tokens owned by the user at a given block level.
        level: int, optional
            The block level where the hDAO snapshot was taken. Default is None,
            which means the current block level.

        """
        # Set the user type as hDAO owner if it has not type defined
        if self.type is None:
            self.type = "hdao_owner"

        self.hdao = hdao
        self.hdao_snapshot_level = level

    def set_usernames(self, registries_bigmap, tzprofiles, wallets):
        """Sets the user names from different sources.

        Parameters
        ----------
        registries_bigmap: dict
            The H=N registries bigmap.
        tzprofiles: dict
            The complete tzprofiles registered users information.
        wallets: dict
            The complete list of tezos wallets obtained from the TzKt API.

        """
        if "alias" in wallets[self.address]:
            self.tzkt_username = wallets[self.address]["alias"].strip()

            if len(self.tzkt_username) > 0:
                self.username = self.tzkt_username

        if self.address in tzprofiles:
            tzprofile = tzprofiles[self.address]

            if tzprofile["alias"] is not None:
                self.tzprofiles_username = tzprofile["alias"].strip()
            elif tzprofile["twitter"] is not None:
                self.tzprofiles_username = tzprofile["twitter"].strip()
            elif tzprofile["discord"] is not None:
                self.tzprofiles_username = tzprofile["discord"].strip()
            elif tzprofile["github"] is not None:
                self.tzprofiles_username = tzprofile["github"].strip()

            if ((self.tzprofiles_username is not None) and 
                (len(self.tzprofiles_username) > 0)):
                self.username = self.tzprofiles_username

        if self.address in registries_bigmap:
            self.hen_username = registries_bigmap[self.address]["user"].strip()

            if len(self.hen_username) > 0:
                self.username = self.hen_username

    def add_mint_transaction(self, transaction):
        """Updates the user with the information of a new mint transaction.

        Parameters
        ----------
        transaction: str
            The mint transaction information.

        """
        # Set the user type as artist
        self.type = "artist"

        # Get the id of the minted OBJKT and the transaction timestamp
        objkt_id = transaction["parameter"]["value"]["token_id"]
        timestamp = transaction["timestamp"]

        # Check if it's the first user activity
        if ((self.first_activity is None) or
            (timestamp < self.first_activity["timestamp"])):
            self.first_activity = {
                "type": "mint",
                "timestamp": timestamp}

        # Check if it's the last user activity
        if ((self.last_activity is None) or
            (timestamp > self.last_activity["timestamp"])):
            self.last_activity = {
                "type": "mint",
                "timestamp": timestamp}

        # Check if it's the first mint
        if ((self.first_mint is None) or
            (timestamp < self.first_mint["timestamp"])):
            self.first_mint = {
                "id": objkt_id,
                "timestamp": timestamp}

        # Check if it's the last mint
        if ((self.last_mint is None) or
            (timestamp > self.last_mint["timestamp"])):
            self.last_mint = {
                "id": objkt_id,
                "timestamp": timestamp}

        # Add the timestamp and the OBJKT id to their respective lists
        self.mint_timestamps.append(timestamp)
        self.minted_objkts.append(objkt_id)

    def add_collect_transaction(self, transaction, swaps, royalties):
        """Updates the user with the information of a new collect transaction.

        Parameters
        ----------
        transaction: str
            The collect transaction information.
        swaps: dict
            The marketplace swaps bigmap.
        royalties: dict
            The marketplace royalties bigmap.

        """
        # Get the swap id from the parameters passed to the entrypoint
        parameters = transaction["parameter"]["value"]

        if isinstance(parameters, dict):
            swap_id = parameters["swap_id"]
        else:
            swap_id = parameters

        # Get the id of the collected OBJKT, the royalties and the paid amount
        objkt_id = swaps[swap_id]["objkt_id"]
        objkt_royalties = int(royalties[objkt_id]["royalties"]) / 1000
        paid_amount = transaction["amount"] / 1e6

        # Get the OBJKT creator, the seller and the collector addresses
        creator_address = royalties[objkt_id]["issuer"]
        seller_address = swaps[swap_id]["issuer"]
        collector_address = transaction["sender"]["address"]

        # Check if the user is the OBJKT creator
        if self.address == creator_address:
            # Set the user type as artist
            self.type = "artist"

            # Add the money earned in royalties with the collect
            money_earned = paid_amount * objkt_royalties
            self.money_earned_own_objkts.append(money_earned)
            self.total_money_earned_own_objkts += money_earned
            self.total_money_earned += money_earned

            # Add the connection with the collector
            if collector_address in self.collector_connections:
                self.collector_connections[collector_address] += 1
            else:
                self.collector_connections[collector_address] = 1

        # Check if the user is the seller
        if self.address == seller_address:
            # Set the user type as swapper if it's not an artist nor a patron
            if self.type not in ["artist", "patron"]:
                self.type = "swapper"

            # Add the money earned with the sell of the OBJKT
            site_fees = 25 / 1000
            money_earned = paid_amount * (1 - objkt_royalties - site_fees)

            if self.address == creator_address:
                self.money_earned_own_objkts.append(money_earned)
                self.total_money_earned_own_objkts += money_earned
            else:
                self.money_earned_other_objkts.append(money_earned)
                self.total_money_earned_other_objkts += money_earned

            self.total_money_earned += money_earned

        # Check if the user is the collector
        if self.address == collector_address:
            # Set the user type as patron if it's not an artist
            if self.type != "artist":
                self.type = "patron"

            # Get transaction timestamp
            timestamp = transaction["timestamp"]

            # Check if it's the first user activity
            if ((self.first_activity is None) or
                (timestamp < self.first_activity["timestamp"])):
                self.first_activity = {
                    "type": "collect",
                    "timestamp": timestamp}

            # Check if it's the last user activity
            if ((self.last_activity is None) or
                (timestamp > self.last_activity["timestamp"])):
                self.last_activity = {
                    "type": "collect",
                    "timestamp": timestamp}

            # Check if it's the first collect
            if ((self.first_collect is None) or
                (timestamp < self.first_collect["timestamp"])):
                self.first_collect = {
                    "id": objkt_id,
                    "timestamp": timestamp}

            # Check if it's the last collect
            if ((self.last_collect is None) or
                (timestamp > self.last_collect["timestamp"])):
                self.last_collect = {
                    "id": objkt_id,
                    "timestamp": timestamp}

            # Add the timestamp and the OBJKT id to their respective lists
            self.collect_timestamps.append(timestamp)
            self.collected_objkts.append(objkt_id)

            # Add the money spent in the collect
            self.money_spent.append(paid_amount)
            self.total_money_spent += paid_amount

            # Add the connection with the artist
            if creator_address in self.artist_connections:
                self.artist_connections[creator_address] += 1
            else:
                self.artist_connections[creator_address] = 1

    def add_swap_transaction(self, transaction):
        """Updates the user with the information of a new swap transaction.

        Parameters
        ----------
        transaction: str
            The swap transaction information.

        """
        # Set the user type as swapper if it's not an artist nor a patron
        if self.type not in ["artist", "patron"]:
            self.type = "swapper"

        # Get the id of the swapped OBJKT and the transaction timestamp
        objkt_id = transaction["parameter"]["value"]["objkt_id"]
        timestamp = transaction["timestamp"]

        # Check if it's the first user activity
        if ((self.first_activity is None) or
            (timestamp < self.first_activity["timestamp"])):
            self.first_activity = {
                "type": "swap",
                "timestamp": timestamp}

        # Check if it's the last user activity
        if ((self.last_activity is None) or
            (timestamp > self.last_activity["timestamp"])):
            self.last_activity = {
                "type": "swap",
                "timestamp": timestamp}

        # Check if it's the first swap
        if ((self.first_swap is None) or
            (timestamp < self.first_swap["timestamp"])):
            self.first_swap = {
                "id": objkt_id,
                "timestamp": timestamp}

        # Check if it's the last swap
        if ((self.last_swap is None) or
            (timestamp > self.last_swap["timestamp"])):
            self.last_swap = {
                "id": objkt_id,
                "timestamp": timestamp}

        # Add the timestamp and the OBJKT id to their respective lists
        self.swap_timestamps.append(timestamp)
        self.swapped_objkts.append(objkt_id)

    def add_artists_collaborations(self, artists_collaborations,
                                   artists_collaborations_signatures, users):
        """Adds the artists collaborations information to the user.

        Parameters
        ----------
        artists_collaborations: dict
            The artists collaborations origination information.
        artists_collaborations_signatures: dict
            The artists collaborations signatures information.
        users: dict
            A python dictionary with the users information.

        """
        # Loop over the list of artists collaborations
        for address, collaboration in artists_collaborations.items():
            # Set the user type as a collaboration if the addresses coincide
            if self.address == address:
                self.type = "collaboration"

            # Check if the user is one of the collaboration core participants
            if self.address in collaboration["storage"]["coreParticipants"]:
                # Get the user signed OBJKTs
                signed_objkts = set()

                if self.address in artists_collaborations_signatures:
                    signed_objkts = set(
                        artists_collaborations_signatures[self.address])

                # Check if the collaboration minted some OBJKTs
                if address in users and len(users[address].minted_objkts) > 0:
                    # Add the collaboration to the list of user collaborations
                    # if it signed one of the collaboration minted OBJKTs
                    collab = users[address]

                    if len(set(collab.minted_objkts) & signed_objkts) > 0:
                        self.collaborations.append(address)

                    # Associate the collaboration OBJKTs to the user
                    for timestamp, objkt_id in zip(collab.mint_timestamps,
                                                   collab.minted_objkts):
                        # Make sure the user signed the OBJKT
                        if objkt_id not in signed_objkts:
                            continue

                        # Check if it's the first user activity
                        if ((self.first_activity is None) or
                            (timestamp < self.first_activity["timestamp"])):
                            self.first_activity = {
                                "type": "mint",
                                "timestamp": timestamp}

                        # Check if it's the last user activity
                        if ((self.last_activity is None) or
                            (timestamp > self.last_activity["timestamp"])):
                            self.last_activity = {
                                "type": "mint",
                                "timestamp": timestamp}

                        # Check if it's the first mint
                        if ((self.first_mint is None) or
                            (timestamp < self.first_mint["timestamp"])):
                            self.first_mint = {
                                "id": objkt_id,
                                "timestamp": timestamp}
                
                        # Check if it's the last mint
                        if ((self.last_mint is None) or
                            (timestamp > self.last_mint["timestamp"])):
                            self.last_mint = {
                                "id": objkt_id,
                                "timestamp": timestamp}

                        # Add the timestamp and the OBJKT id to the lists
                        self.mint_timestamps.append(timestamp)
                        self.minted_objkts.append(objkt_id)

                    # Add the money earned with the collaboration
                    share = (
                        int(collaboration["storage"]["shares"][self.address]) / 
                        int(collaboration["storage"]["totalShares"]))
                    self.total_money_earned_collaborations_objkts += (
                        share * collab.total_money_earned_own_objkts)
                    self.total_money_earned += (
                        share * collab.total_money_earned)

    def add_teia_community_votes(self, votes, polls):
        """Adds the Teia Community votes associated to the user.

        Parameters
        ----------
        votes: dict
            The Teia Community votes information.
        polls: list
            The list of polls to consider.

        """
        if self.address in votes:
            for poll, vote in votes[self.address].items():
                if poll in polls:
                    self.teia_community_votes[poll] = vote

    def compress_connections(self, users):
        """Compresses the artist and collector connections information using the
        user ids.

        Parameters
        ----------
        users: dict
            A python dictionary with the users information.

        """
        # Compress the artist connections
        compressed_connections = {}

        for address, connections in self.artist_connections.items():
            compressed_connections[users[address].id] = connections

        self.artist_connections = compressed_connections

        # Compress the collector connections
        compressed_connections = {}

        for address, connections in self.collector_connections.items():
            compressed_connections[users[address].id] = connections

        self.collector_connections = compressed_connections

    def __str__(self):
        """Prints the instance attributes.

        Returns
        -------
        str
            A string representation of all the instance attributes.

        """
        attributeList = []

        for attribute in self.__dict__:
            attributeList.append("%s = %s" % (
                attribute, getattr(self, attribute)))

        return "\n".join(attributeList)


class TeiaUsers:
    """This class collects all the information associated with a set of Teia
    users.

    """

    def __init__(self, users=None):
        """The class constructor.

        Parameters
        ----------
        users: dict, optional
            A python dictionary with a set of Teia users. Default is None.

        """
        self.users = {} if users is None else users
        self.id_to_address = {
            user.id: user.address for user in self.users.values()}

    def __len__(self):
        """Returns the users length.

        """
        return len(self.users)

    def __getitem__(self, address):
        """Returns the user associated with the given address.

        """
        return self.users[address]

    def __iter__(self):
        """Returns the users iterator.

        """
        return self.users.__iter__()

    def keys(self):
        """Returns the users keys.

        """
        return self.users.keys()

    def values(self):
        """Returns the users values.

        """
        return self.users.values()

    def items(self):
        """Returns the users items.

        """
        return self.users.items()

    def get_user(self, address):
        """Returns the user connected to the given address.

        """
        return self.users[address]

    def get_user_by_id(self, id):
        """Returns the user connected to the given id.

        """
        return self.users[self.id_to_address[id]]

    def add_mint_transactions(self, mint_transactions, mint_objkt_transactions):
        """Adds the mint transactions information to the users.

        Parameters
        ----------
        mint_transactions: list
            The list of mint transactions.
        mint_objkt_transactions: list
            The list of mint_OBJKT transactions.

        """
        # Loop over the list of transactions
        for mint, mint_objkt in zip(mint_transactions, mint_objkt_transactions):
            # Extract the minter address
            address = mint_objkt["sender"]["address"]

            # Add a new user if the address is new
            if address not in self.users:
                id = len(self.users)
                self.users[address] = TeiaUser(address, id)
                self.id_to_address[id] = address

            # Add the mint transaction to the user information
            self.users[address].add_mint_transaction(mint)

    def add_collect_transactions(self, transactions, swaps, royalties):
        """Adds the collect transactions information to the users.

        Parameters
        ----------
        transactions: list
            The list of collect transactions.
        swaps: dict
            The marketplace swaps bigmap.
        royalties: dict
            The marketplace royalties bigmap.

        """
        # Loop over the list of collect transactions
        for transaction in transactions:
            # Get the swap id from the parameters passed to the entrypoint
            parameters = transaction["parameter"]["value"]

            if isinstance(parameters, dict):
                swap_id = parameters["swap_id"]
            else:
                swap_id = parameters

            # Get the OBJKT creator, the seller and the collector addresses
            creator_address = royalties[swaps[swap_id]["objkt_id"]]["issuer"]
            seller_address = swaps[swap_id]["issuer"]
            collector_address = transaction["sender"]["address"]

            # Loop over the unique addresses
            addresses = {creator_address, seller_address, collector_address}

            for address in addresses:
                # Add a new user if the address is new
                if address not in self.users:
                    id = len(self.users)
                    self.users[address] = TeiaUser(address, id)
                    self.id_to_address[id] = address

                # Add the collect transaction to the user information
                self.users[address].add_collect_transaction(
                    transaction, swaps, royalties)

    def add_swap_transactions(self, transactions):
        """Adds the swap transactions information to the users.

        Parameters
        ----------
        transactions: list
            The list of swap transactions.

        """
        # Loop over the list of swap transactions
        for transaction in transactions:
            # Extract the swapper address
            address = transaction["sender"]["address"]

            # Add a new user if the address is new
            if address not in self.users:
                id = len(self.users)
                self.users[address] = TeiaUser(address, id)
                self.id_to_address[id] = address

            # Add the swap transaction to the user information
            self.users[address].add_swap_transaction(transaction)

    def add_hdao_information(self, hdao_ledger, level):
        """Adds the hDAO information to the users.

        Parameters
        ----------
        hdao_ledger: dict
            The hDAO ledger bigmap.
        level: dict
            The block level when the hDAO ledger bigmap snapshot has been taken.

        """
        for address, hdao in hdao_ledger.items():
            # Check that the account still owns some hDAO
            if int(hdao) > 0:
                # Add a new user if the address is new
                if address not in self.users:
                    id = len(self.users)
                    self.users[address] = TeiaUser(address, id)
                    self.id_to_address[id] = address
    
                # Set the user hDAO amount
                self.users[address].set_hdao(int(hdao), level)

    def add_restricted_addresses_information(self, restricted_addresses):
        """Adds the restricted addresses information to the users.

        Parameters
        ----------
        restricted_addresses: list
            The python list with the Teia restricted addresses.

        """
        for address, user in self.users.items():
            user.set_restricted(address in restricted_addresses)

    def add_usernames(self, registries_bigmap, tzprofiles, wallets):
        """Adds the user names information to the users.

        Parameters
        ----------
        registries_bigmap: dict
            The H=N registries bigmap.
        tzprofiles: dict
            The complete tzprofiles registered users information.
        wallets: dict
            The complete list of tezos wallets obtained from the TzKt API.

        """
        for address, user in self.users.items():
            user.set_usernames(registries_bigmap, tzprofiles, wallets)

    def add_artists_collaborations(self, artists_collaborations,
                                   artists_collaborations_signatures):
        """Adds the artists collaborations information to the users.

        Parameters
        ----------
        artists_collaborations: dict
            The artists collaborations origination information.
        artists_collaborations_signatures: dict
            The artists collaborations signatures information.

        """
        for user in self.users.values():
            user.add_artists_collaborations(
                artists_collaborations, artists_collaborations_signatures,
                self.users)

    def add_teia_community_votes(self, votes, polls):
        """Adds the Teia Community votes information to the users.

        Parameters
        ----------
        votes: dict
            The Teia Community votes information.
        polls: list
            The list of polls to consider.

        """
        for user in self.users.values():
            user.add_teia_community_votes(votes, polls)

    def compress_user_connections(self):
        """Compresses the user connections information using the user ids
        instead of their addresses.

        """
        for user in self.users.values():
            user.compress_connections(self.users)

    def select(self, filter_selection):
        """Selects the users by a given filter selection.

        Parameters
        ----------
        filter_selection: string
            The filter selection: artists, patrons, collectors, swappers,
            hdao_owners, restricted, not_restricted, collaborations, contract,
            not_contract.

        """
        selected_users = {}

        if filter_selection == "artists":
            for address, user in self.users.items():
                if user.type == "artist":
                    selected_users[address] = user

        if filter_selection == "collectors":
            for address, user in self.users.items():
                if len(user.collected_objkts) > 0:
                    selected_users[address] = user

        if filter_selection == "patrons":
            for address, user in self.users.items():
                if user.type == "patron":
                    selected_users[address] = user

        if filter_selection == "swappers":
            for address, user in self.users.items():
                if user.type == "swapper":
                    selected_users[address] = user

        if filter_selection == "hdao_owners":
            for address, user in self.users.items():
                if user.type == "hdao_owner":
                    selected_users[address] = user

        if filter_selection == "not_hdao_owners":
            for address, user in self.users.items():
                if user.type != "hdao_owner":
                    selected_users[address] = user

        if filter_selection == "restricted":
            for address, user in self.users.items():
                if user.restricted:
                    selected_users[address] = user

        if filter_selection == "not_restricted":
            for address, user in self.users.items():
                if not user.restricted:
                    selected_users[address] = user

        if filter_selection == "collaborations":
            for address, user in self.users.items():
                if user.type == "collaboration":
                    selected_users[address] = user

        if filter_selection == "contract":
            for address, user in self.users.items():
                if address.startswith("KT"):
                    selected_users[address] = user

        if filter_selection == "not_contract":
            for address, user in self.users.items():
                if address.startswith("tz"):
                    selected_users[address] = user

        return TeiaUsers(selected_users)

    def get_top_selling_artists(self, n):
        """Returns the addresses of the top selling artists.

        Restricted users are not considered.

        Parameters
        ----------
        n: int
            The number of artists to return.

        Returns
        -------
        object
            A numpy array with the top selling artists addresses.

        """
        # Get the addresses and the total earned money by each user selling
        # their own OBJKTs
        addresses = np.array([
            user.address for user in self.users.values()
            if not user.restricted]) 
        total_money_earned_own_objkts = np.array([
            user.total_money_earned_own_objkts for user in self.users.values()
            if not user.restricted])

        # Return the user addresses ordered by the total money earned
        return addresses[total_money_earned_own_objkts.argsort()[::-1]][:n]

    def get_top_collectors(self, n):
        """Returns the addresses of the top collectors ordered by the money they
        spent.

        Restricted users are not considered.

        Parameters
        ----------
        n: int
            The number of collectors to return.

        Returns
        -------
        object
            A numpy array with the top collectors addresses.

        """
        # Get the addresses and the total money spent by each user
        addresses = np.array([
            user.address for user in self.users.values()
            if not user.restricted]) 
        total_money_spent = np.array([
            user.total_money_spent for user in self.users.values()
            if not user.restricted])

        # Return the user addresses ordered by the total money spent
        return addresses[total_money_spent.argsort()[::-1]][:n]

    def save_as_csv_file(self, file_name):
        """Saves the most relevant user information in a csv file.

        Parameters
        ----------
        file_name: str
            The complete path to the csv file where the users information
            should be saved.

        """
        # Define the output file columns and their format
        columns = [
            "username", "address", "type", "restricted", "has_tzprofile",
            "has_hen_profile", "has_tzkt_profile", "hdao", "first_activity",
            "last_activity", "first_mint", "last_mint", "first_collect",
            "last_collect", "first_swap", "last_swap", "activity_period",
            "active_days", "minted_objkts", "collected_objkts",
            "swapped_objkts", "money_earned_own_objkts",
            "money_earned_collaborations_objkts", "money_earned_other_objkts",
            "money_earned", "money_spent", "collaborations",
            "connections_to_artists", "connections_to_collectors",
            "connections_to_users", "teia_votes"]
        format = [
            "%s", "%s", "%s", "%r", "%r", "%r", "%r", "%f", "%s", "%s", "%s",
            "%s", "%s", "%s", "%s", "%s", "%f", "%i", "%i", "%i", "%i", "%f",
            "%f", "%f", "%f", "%f", "%i", "%i", "%i", "%i", "%i"]

        with open(file_name, "w") as file:
            # Write the header
            file.write(",".join(columns) + "\n")

            # Loop over the users
            for user in self.users.values():
                # Set the username
                username = user.address if user.username is None else user.username

                # Get the activity timestamps
                first_activity = ""
                last_activity = ""
                first_mint = ""
                last_mint = ""
                first_collect = ""
                last_collect = ""
                first_swap = ""
                last_swap = ""

                if user.first_activity is not None:
                    first_activity = user.first_activity["timestamp"]
                    last_activity = user.last_activity["timestamp"]

                if user.first_mint is not None:
                    first_mint = user.first_mint["timestamp"]
                    last_mint = user.last_mint["timestamp"]

                if user.first_collect is not None:
                    first_collect = user.first_collect["timestamp"]
                    last_collect = user.last_collect["timestamp"]

                if user.first_swap is not None:
                    first_swap = user.first_swap["timestamp"]
                    last_swap = user.last_swap["timestamp"]

                # Calculate the user active period
                active_period = 0

                if first_activity != "":
                    active_period = (
                        get_datetime_from_timestamp(last_activity) - 
                        get_datetime_from_timestamp(first_activity)
                        ).total_seconds() / (3600 * 24)

                # Calculate how many days the user have been active
                active_days = {
                    timestamp[:10] for timestamp in user.mint_timestamps}
                active_days |= {
                    timestamp[:10] for timestamp in user.collect_timestamps}
                active_days |= {
                    timestamp[:10] for timestamp in user.swap_timestamps}

                # Write the user data in the output file
                data = (
                    username.replace(",", "_").replace(";", "_"),
                    user.address,
                    user.type,
                    user.restricted,
                    user.tzprofiles_username is not None,
                    user.hen_username is not None,
                    user.tzkt_username is not None,
                    user.hdao / 1e6,
                    first_activity,
                    last_activity,
                    first_mint,
                    last_mint,
                    first_collect,
                    last_collect,
                    first_swap,
                    last_swap,
                    active_period,
                    len(active_days),
                    len(set(user.minted_objkts)),
                    len(set(user.collected_objkts)),
                    len(set(user.swapped_objkts)),
                    user.total_money_earned_own_objkts,
                    user.total_money_earned_collaborations_objkts,
                    user.total_money_earned_other_objkts,
                    user.total_money_earned,
                    user.total_money_spent,
                    len(user.collaborations),
                    len(user.artist_connections),
                    len(user.collector_connections),
                    len(set(list(user.artist_connections.keys()) + 
                            list(user.collector_connections.keys()))),
                    len(user.teia_community_votes))
                text = ",".join(format) % data
                file.write(text + "\n")
