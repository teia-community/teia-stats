

class TeiaUser:
    """This class collects all the information associated with a Teia user.

    """

    def __init__(self, address):
        """The class constructor.

        Parameters
        ----------
        address: str
            The user tz address.

        """
        # General information
        self.address = address
        self.type = None
        self.restricted = False

        # User names
        self.username = None
        self.tzkt_username = None
        self.tzprofiles_username = None
        self.hen_username = None

        # hDAO information
        self.hdao = None
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
        self.total_money_earned_other_objkts = 0
        self.total_money_earned = 0
        self.total_money_spent = 0

        # Connections with other users
        self.artist_connections = {}
        self.collector_connections = {}

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
            self.tzkt_username = wallets[self.address]["alias"]
            self.username = self.tzkt_username

        if self.address in tzprofiles:
            self.tzprofiles_username = tzprofiles[self.address]["alias"]
            self.username = self.tzprofiles_username

        if self.address in registries_bigmap:
            self.hen_username = registries_bigmap[self.address]["user"]
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

    def add_mint_transactions(self, transactions):
        """Adds the mint transactions information to the users.

        Parameters
        ----------
        transactions: list
            The list of mint transactions.

        """
        # Loop over the list of mint transactions
        for transaction in transactions:
            # Extract the minter address
            address = transaction["initiator"]["address"]

            # Check that it is a normal address and not a contract
            if address.startswith("tz"):
                # Add a new user if the address is new
                if address not in self.users:
                    self.users[address] = TeiaUser(address)

                # Add the mint transaction to the user information
                self.users[address].add_mint_transaction(transaction)

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
                # Check that it is a normal address and not a contract
                if address.startswith("tz"):
                    # Add a new user if the address is new
                    if address not in self.users:
                        self.users[address] = TeiaUser(address)

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

            # Check that it is a normal address and not a contract
            if address.startswith("tz"):
                # Add a new user if the address is new
                if address not in self.users:
                    self.users[address] = TeiaUser(address)

                # Add the swap transaction to the user information
                self.users[address].add_swap_transaction(transaction)

    def add_restricted_wallets_information(self, restricted_wallets):
        """Adds the restricted wallets information to the users.

        Parameters
        ----------
        restricted_wallets: list
            The python list with the Teia restricted wallets.

        """
        for wallet, user in self.users.items():
            user.set_restricted(wallet in restricted_wallets)

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
        for wallet, user in self.users.items():
            user.set_usernames(registries_bigmap, tzprofiles, wallets)

    def add_hdao_information(self, hdao_ledger, level):
        """Adds the hDAO information to the users.

        Parameters
        ----------
        hdao_ledger: dict
            The hDAO ledger bigmap.
        level: dict
            The block level when the hDAO ledger bigmap snapshot has been taken.

        """
        for wallet, user in self.users.items():
            if wallet in hdao_ledger:
                user.set_hdao(int(hdao_ledger[wallet]), level)
            else:
                user.set_hdao(0, level)

    def select(self, filter_selection):
        """Selects the users by a given filter selection.

        Parameters
        ----------
        filter_selection: string
            The filter selection: artists, patrons, collectors, swappers,
            restricted, not_restricted.

        """
        selected_users = {}

        if filter_selection == "artists":
            for wallet, user in self.users.items():
                if user.type == "artist":
                    selected_users[wallet] = user

        if filter_selection == "collectors":
            for wallet, user in self.users.items():
                if len(user.collected_objkts) > 0:
                    selected_users[wallet] = user

        if filter_selection == "patrons":
            for wallet, user in self.users.items():
                if user.type == "patron":
                    selected_users[wallet] = user

        if filter_selection == "swappers":
            for wallet, user in self.users.items():
                if user.type == "swapper":
                    selected_users[wallet] = user

        if filter_selection == "restricted":
            for wallet, user in self.users.items():
                if user.restricted:
                    selected_users[wallet] = user

        if filter_selection == "not_restricted":
            for wallet, user in self.users.items():
                if not user.restricted:
                    selected_users[wallet] = user

        return TeiaUsers(selected_users)
