import json
import requests
import time
import os.path
import numpy as np
from datetime import datetime, timezone
from calendar import monthrange

import teiaUtils.analysisUtils as utils


def get_query_result(url, parameters={}, timeout=10):
    """Executes the given query and returns the result.

    Parameters
    ----------
    url: str
        The url to the server API.
    parameters: dict, optional
        The query parameters. Default is no parameters.
    timeout: float, optional
        The query timeout in seconds. Default is 10 seconds.

    Returns
    -------
    object
        The query result.

    """
    response = requests.get(url=url, params=parameters, timeout=timeout)

    if response.status_code == requests.codes.ok:
        return response.json()

    return None


def get_graphql_query_result(url, query, timeout=10):
    """Executes the given GraphQL query and returns the result.

    Parameters
    ----------
    url: str
        The url to the GraphQL server API.
    query: dict
        The GraphQL query.
    timeout: float, optional
        The query timeout in seconds. Default is 10 seconds.

    Returns
    -------
    object
        The query result.

    """
    response = requests.post(url=url, data=json.dumps(query), timeout=timeout)

    if response.status_code == requests.codes.ok:
        return response.json()

    return None


def get_tez_exchange_rates(coin, start_date="2018-10-16T00:00:00Z",
                           end_date=None, sampling="1d"):
    """Returns the tez exchange rates for a given time range and sampling
    interval.

    Parameters
    ----------
    coin: str
        The coin to consider: USD or EUR.
    start_date: str, optional
        The start date.  It should follow the ISO format (e.g.
        2021-04-20T00:00:00Z). Default is the first day with exchange rate data
        (2018-10-16T00:00:00Z).
    end_date: str, optional
        The end date. It should follow the ISO format (e.g.
        2021-04-20T00:00:00Z). Default is today.
    sampling: str, optional
        The sampling interval: 1m, 5m, 15m, 30m, 1h, 2h, 3h, 4h, 6h, 12h, 1d,
        1w, 1M, 3M or 1y. Default is 1d.

    Returns
    -------
    tuple
        A python tuple with the lists of time stamps and exchange rates.

    """
    # Get the exchange rate information
    url = "https://api.tzstats.com/series/kraken/XTZ_%s/ohlcv" % coin
    parameters = {
        "start_date": start_date,
        "end_date": end_date,
        "collapse": sampling,
        "limit": 500000,
        "columns": "time,open,close"
    }
    exchange_rate_information = get_query_result(url, parameters)

    # Get the time stamps
    datetime_format = "%Y-%m-%dT%H:%M:%SZ"
    timestamps = [
        datetime.fromtimestamp(
            row[0] / 1000, tz=timezone.utc).strftime(datetime_format)
        for row in exchange_rate_information]

    # Calculate the average exchange rates
    exchange_rates = [
        (row[1] + row[2]) / 2 for row in exchange_rate_information]

    return timestamps, exchange_rates


def get_restricted_users():
    """Returns the list of restricted users stored in the Teia Community github
    repository.

    Returns
    -------
    list
        A python list with the wallet ids of all the restricted users.

    """
    github_repository = "teia-community/teia-report"
    file_path = "restricted.json"
    url = "https://raw.githubusercontent.com/%s/main/%s" % (
        github_repository, file_path)

    return list(set(get_query_result(url)))


def extract_relevant_wallet_information(wallets):
    """Extracts the most relevant information from a list of tezos wallets.

    This is mostly done to save memory.

    Parameters
    ----------
    wallets: list
        The list of wallets.

    Returns
    -------
    list
        A python list with the most relevant wallets information.

    """
    keys = [
        "type", "address", "alias", "balance", "firstActivityTime",
        "lastActivityTime"]

    return [utils.select_keys(wallet, keys) for wallet in wallets]


def extract_relevant_transaction_information(transactions):
    """Extracts the most relevant information from a list of transactions.

    This is mostly done to save memory.

    Parameters
    ----------
    transactions: list
        The list of transactions.

    Returns
    -------
    list
        A python list with the most relevant transactions information.

    """
    keys = [
        "timestamp", "level", "initiator", "sender", "target", "amount",
        "parameter"]

    return [utils.select_keys(transaction, keys) for transaction in transactions]


def get_tezos_wallets(data_dir, batch_size=10000, sleep_time=1):
    """Returns the complete list of tezos wallets ordered by increasing first
    activity.

    Parameters
    ----------
    data_dir: str
        The complete path to the directory where the wallets information should
        be saved.
    batch_size: int, optional
        The maximum number of wallets per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the wallets information.

    """
    # Download the wallets
    utils.print_info("Downloading the complete list of tezos wallets...")
    wallets = []
    counter = 0

    while True:
        offset = counter * batch_size
        batch = counter + 1

        file_name = os.path.join(
            data_dir, "wallets_%i-%i.json" % (offset, offset + batch_size))

        if os.path.exists(file_name):
            utils.print_info(
                "Batch %i has been already downloaded. Reading it from local "
                "json file." % batch)
            wallets += extract_relevant_wallet_information(
                utils.read_json_file(file_name))
        else:
            utils.print_info("Downloading batch %i" % batch)
            url = "https://api.tzkt.io/v1/accounts"
            parameters = {
                "sort": "firstActivity",
                "offset": offset,
                "limit": batch_size
            }
            new_wallets = get_query_result(url, parameters)
            wallets += extract_relevant_wallet_information(new_wallets)

            if len(new_wallets) != batch_size:
                break

            utils.print_info("Saving batch %i in the output directory" % batch)
            utils.save_json_file(file_name, new_wallets)

            time.sleep(sleep_time)

        counter += 1

    utils.print_info("Downloaded %i wallets." % len(wallets))

    return {wallet["address"]: wallet for wallet in wallets}


def get_transactions(entrypoint, contract, offset=0, limit=10000,
                     timestamp=None, extra_parameters={}):
    """Returns a list of applied transactions ordered by increasing time stamp.

    Parameters
    ----------
    entrypoint: str
        The contract entrypoint (e.g. mint, collect, swap, cancel_swap).
    contract: str
        The contract address.
    offset: int, optional
        The number of initial transactions that should be skipped. This is
        mostly used for pagination. Default is 0.
    limit: int, optional
        The maximum number of transactions to return. Default is 10000. The
        maximum allowed by the API is 10000.
    timestamp: str, optional
        The maximum transaction time stamp. Only earlier transactions will be
        returned. It should follow the ISO format (e.g. 2021-04-20T00:00:00Z).
        Default is no limit.
    extra_parameters: dict, optional
        Extra parameters to apply to the query. Default is no extra parameters.

    Returns
    -------
    list
        A python list with the transactions information.

    """
    url = "https://api.tzkt.io/v1/operations/transactions"
    parameters = {
        "target": contract,
        "status": "applied",
        "entrypoint": entrypoint,
        "offset": offset,
        "limit": limit,
        "timestamp.le": timestamp
    }
    parameters.update(extra_parameters)

    return get_query_result(url, parameters)


def get_all_transactions(type, data_dir, batch_size=10000, sleep_time=1):
    """Returns the complete list of applied transactions of a given type
    ordered by increasing time stamp.

    Parameters
    ----------
    type: str
        The transaction type (e.g. mint, hen_collect, teia_swap).
    data_dir: str
        The complete path to the directory where the transactions information
        should be saved.
    batch_size: int, optional
        The maximum number of transactions per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    list
        A python list with the transactions information.

    """
    # Set the contract addresses and the entrypoint
    if type in "mint":
        contracts = ["KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton"]
        entrypoint = type
    elif type in "mint_OBJKT":
        contracts = ["KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9"]
        entrypoint = type
    elif type in ["hen_collect", "hen_swap", "hen_cancel_swap"]:
        contracts = ["KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9",
                     "KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn"]
        entrypoint = type.replace("hen_", "")
    elif type in ["teia_collect", "teia_swap", "teia_cancel_swap"]:
        contracts = ["KT1PHubm9HtyQEJ4BBpMTVomq6mhbfNZ9z5w"]
        entrypoint = type.replace("teia_", "")
    else:
        raise ValueError("Invalid type parameter value: %s" % type)

    # Download the transactions
    utils.print_info("Downloading %s transactions..." % type)
    transactions = []
    counter = 0
    total_counter = 0

    for contract in contracts:
        while True:
            offset = counter * batch_size
            batch = total_counter + 1

            file_name = os.path.join(
                data_dir, "%s_transactions_%s_%i-%i.json" % (
                    type, contract, offset, offset + batch_size))

            if os.path.exists(file_name):
                utils.print_info(
                    "Batch %i has been already downloaded. Reading it from "
                    "local json file." % batch)
                transactions += extract_relevant_transaction_information(
                    utils.read_json_file(file_name))
            else:
                utils.print_info("Downloading batch %i" % batch)
                new_transactions = get_transactions(
                    entrypoint, contract, offset, batch_size)
                transactions += extract_relevant_transaction_information(
                    new_transactions)

                if len(new_transactions) != batch_size:
                    counter = 0
                    total_counter += 1
                    break

                utils.print_info(
                    "Saving batch %i in the output directory" % batch)
                utils.save_json_file(file_name, new_transactions)

                time.sleep(sleep_time)

            counter += 1
            total_counter += 1

    utils.print_info(
        "Downloaded %i %s transactions." % (len(transactions), type))

    return transactions


def get_bigmap_keys(bigmap_ids, data_dir, level=None, batch_size=10000,
                    sleep_time=1):
    """Returns the complete bigmap key list.

    Parameters
    ----------
    bigmap_ids: list
        A list with the bigmap ids to query.
    data_dir: str
        The complete path to the directory where the bigmap keys information
        should be saved.
    level: int, optional
        The block lever to check. Default is the current block level.
    batch_size: int, optional
        The maximum number of bigmap keys per API query. Default is 10000.
        The maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    list
        A python list with the bigmap keys.

    """
    # Download the bigmap keys
    utils.print_info("Downloading bigmap keys...")
    bigmap_keys = []
    counter = 0
    total_counter = 0

    for bigmap_id in bigmap_ids:
        while True:
            offset = counter * batch_size
            batch = total_counter + 1

            if level is None:
                file_name = os.path.join(
                    data_dir, "bigmap_keys_%s_%i-%i.json" % (
                        bigmap_id, offset, offset + batch_size))
            else:
                file_name = os.path.join(
                    data_dir, "bigmap_keys_%s_%i_%i-%i.json" % (
                        bigmap_id, level, offset, offset + batch_size))

            if os.path.exists(file_name):
                utils.print_info(
                    "Batch %i has been already downloaded. Reading it from "
                    "local json file." % batch)
                bigmap_keys += utils.read_json_file(file_name)
            else:
                utils.print_info("Downloading batch %i" % batch)
                if level is None:
                    url = "https://api.tzkt.io/v1/bigmaps/%s/keys" % bigmap_id
                else:
                    url = "https://api.tzkt.io/v1/bigmaps/%s/historical_keys/%i" % (bigmap_id, level)
                parameters = {
                    "offset": offset,
                    "limit": batch_size
                }
                new_bigmap_keys = get_query_result(url, parameters)
                bigmap_keys += new_bigmap_keys

                if len(new_bigmap_keys) != batch_size:
                    counter = 0
                    total_counter += 1
                    break

                utils.print_info(
                    "Saving batch %i in the output directory" % batch)
                utils.save_json_file(file_name, new_bigmap_keys)

                time.sleep(sleep_time)

            counter += 1
            total_counter += 1

    utils.print_info("Downloaded %i bigmap keys." % len(bigmap_keys))

    return bigmap_keys


def get_hen_bigmap(name, data_dir, level=None, batch_size=10000, sleep_time=1):
    """Returns one of the HEN bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: swaps, royalties, registries, subjkts metadata.
    data_dir: str
        The complete path to the directory where the HEN bigmap keys information
        should be saved.
    level: int, optional
        The block lever to check. Default is the current block level.
    batch_size: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the HEN bigmap.

    """
    # Get the HEN bigmap ids
    if name == "swaps":
        bigmap_ids = [
            "523",  # KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9
            "6072"  # KT1HbQepzV1nVGg8QVznG7z4RcHseD5kwqBn
        ]
    elif name == "royalties":
        bigmap_ids = [
            "522"  # KT1Hkg5qeNhfwpKW4fXvq7HGZB9z2EnmCCA9
        ]
    elif name == "registries":
        bigmap_ids = [
            "3919"  # KT1My1wDZHDGweCrJnQJi3wcFaS67iksirvj
        ]
    elif name == "subjkts metadata":
        bigmap_ids = [
            "3921"  # KT1My1wDZHDGweCrJnQJi3wcFaS67iksirvj
        ]
    else:
        raise ValueError("Invalid name parameter value: %s" % name)

    # Get the HEN bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, level, batch_size, sleep_time)

    # Build the bigmap
    bigmap = {}

    for bigmap_key in bigmap_keys:
        if name in ["swaps", "royalties"]:
            key = bigmap_key["key"]
            bigmap[key] = bigmap_key["value"]
        elif name == "registries":
            key = bigmap_key["key"]
            value = utils.hex_to_utf8(bigmap_key["value"])
            bigmap[key] = {"user": value}
        elif name == "subjkts metadata":
            key = utils.hex_to_utf8(bigmap_key["key"])
            value = utils.hex_to_utf8(bigmap_key["value"])
            bigmap[key] = {"user_metadata": value}

        bigmap[key]["active"] = bigmap_key["active"]

    return bigmap


def get_teia_bigmap(name, data_dir, level=None, batch_size=10000, sleep_time=1):
    """Returns one of the Teia bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: swaps or allowed_fa2s.
    data_dir: str
        The complete path to the directory where the Teia bigmap keys
        information should be saved.
    level: int, optional
        The block lever to check. Default is the current block level.
    batch_size: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the Teia bigmap.

    """
    # Get the HEN bigmap ids
    if name == "swaps":
        bigmap_ids = [
            "90366",  # KT1PHubm9HtyQEJ4BBpMTVomq6mhbfNZ9z5w
        ]
    elif name == "allowed_fa2s":
        bigmap_ids = [
            "90364"  # KT1PHubm9HtyQEJ4BBpMTVomq6mhbfNZ9z5w
        ]
    else:
        raise ValueError("Invalid name parameter value: %s" % name)

    # Get the Teia bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, level, batch_size, sleep_time)

    # Build the bigmap
    bigmap = {}

    for bigmap_key in bigmap_keys:
        key = bigmap_key["key"]
        bigmap[key] = bigmap_key["value"]
        bigmap[key]["active"] = bigmap_key["active"]

    return bigmap


def get_token_bigmap(name, token, data_dir, level=None, batch_size=10000,
                     sleep_time=1):
    """Returns one of the token bigmaps.

    Parameters
    ----------
    name: str
        The bigmap name: ledger, token_metadata, operators.
    token: str
        The token name: OBJKT, hDAO, tezzardz, prjktneon, artcardz, gogo, neonz,
        skele, GENTK, ZIGGURATS, ITEM, MATERIA.
    data_dir: str
        The complete path to the directory where the token bigmap keys
        information should be saved.
    level: int, optional
        The block lever to check. Default is the current block level.
    batch_size: int, optional
        The maximum number of bigmap keys per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the token bigmap.

    """
    # Set the token bigmap ids
    if name == "ledger":
        if token == "OBJKT":
            bigmap_ids = ["511"]  # KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
        elif token == "hDAO":
            bigmap_ids = ["515"]  # KT1AFA2mwNUMNd4SsujE1YYp29vd8BZejyKW
        elif token == "tezzardz":
            bigmap_ids = ["12112"]  # KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6
        elif token == "prjktneon":
            bigmap_ids = ["16117"]  # KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ
        elif token == "artcardz":
            bigmap_ids = ["19083"]  # KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C
        elif token == "gogo":
            bigmap_ids = ["20608"]  # KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ
        elif token == "neonz":
            bigmap_ids = ["21217"]  # KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ
        elif token == "skele":
            bigmap_ids = ["22381"]  # KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih
        elif token == "GENTK":
            bigmap_ids = ["22785"]  # KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE
        elif token == "ZIGGURATS":
            bigmap_ids = ["42519"]  # KT1PNcZQkJXMQ2Mg92HG1kyrcu3auFX5pfd8
        elif token == "ITEM":
            bigmap_ids = ["75550"]  # KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW
        elif token == "MATERIA":
            bigmap_ids = ["76310"]  # KT1KRvNVubq64ttPbQarxec5XdS6ZQU4DVD2
        else:
            raise ValueError("Invalid token parameter value: %s" % token)
    elif name == "token_metadata":
        if token == "OBJKT":
            bigmap_ids = ["514"]  # KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
        elif token == "hDAO":
            bigmap_ids = ["518"]  # KT1AFA2mwNUMNd4SsujE1YYp29vd8BZejyKW
        elif token == "tezzardz":
            bigmap_ids = ["12115"]  # KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6
        elif token == "prjktneon":
            bigmap_ids = ["16120"]  # KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ
        elif token == "artcardz":
            bigmap_ids = ["19086"]  # KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C
        elif token == "gogo":
            bigmap_ids = ["20611"]  # KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ
        elif token == "neonz":
            bigmap_ids = ["21220"]  # KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ
        elif token == "skele":
            bigmap_ids = ["22384"]  # KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih
        elif token == "GENTK":
            bigmap_ids = ["22789"]  # KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE
        elif token == "ZIGGURATS":
            bigmap_ids = ["42521"]  # KT1PNcZQkJXMQ2Mg92HG1kyrcu3auFX5pfd8
        elif token == "ITEM":
            bigmap_ids = ["75556"]  # KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW
        elif token == "MATERIA":
            bigmap_ids = ["76314"]  # KT1KRvNVubq64ttPbQarxec5XdS6ZQU4DVD2
        else:
            raise ValueError("Invalid token parameter value: %s" % token)
    elif name == "operators":
        if token == "OBJKT":
            bigmap_ids = ["513"]  # KT1RJ6PbjHpwc3M5rw5s2Nbmefwbuwbdxton
        elif token == "hDAO":
            bigmap_ids = ["517"]  # KT1AFA2mwNUMNd4SsujE1YYp29vd8BZejyKW
        elif token == "tezzardz":
            bigmap_ids = ["12114"]  # KT1LHHLso8zQWQWg1HUukajdxxbkGfNoHjh6
        elif token == "prjktneon":
            bigmap_ids = ["16119"]  # KT1VbHpQmtkA3D4uEbbju26zS8C42M5AGNjZ
        elif token == "artcardz":
            bigmap_ids = ["19085"]  # KT1LbLNTTPoLgpumACCBFJzBEHDiEUqNxz5C
        elif token == "gogo":
            bigmap_ids = ["20610"]  # KT1SyPgtiXTaEfBuMZKviWGNHqVrBBEjvtfQ
        elif token == "neonz":
            bigmap_ids = ["21219"]  # KT1MsdyBSAMQwzvDH4jt2mxUKJvBSWZuPoRJ
        elif token == "skele":
            bigmap_ids = ["22383"]  # KT1HZVd9Cjc2CMe3sQvXgbxhpJkdena21pih
        elif token == "GENTK":
            bigmap_ids = ["22787"]  # KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE
        elif token == "ZIGGURATS":
            bigmap_ids = ["42520"]  # KT1PNcZQkJXMQ2Mg92HG1kyrcu3auFX5pfd8
        elif token == "ITEM":
            bigmap_ids = ["75553"]  # KT1LjmAdYQCLBjwv4S2oFkEzyHVkomAf5MrW
        elif token == "MATERIA":
            bigmap_ids = ["76312"]  # KT1KRvNVubq64ttPbQarxec5XdS6ZQU4DVD2
        else:
            raise ValueError("Invalid token parameter value: %s" % token)
    else:
        raise ValueError("Invalid name parameter value: %s" % name)

    # Get the token bigmap keys
    bigmap_keys = get_bigmap_keys(
        bigmap_ids, data_dir, level, batch_size, sleep_time)

    # Build the bigmap
    bigmap = {}
    counter = 0

    for bigmap_key in bigmap_keys:
        if isinstance(bigmap_key["key"], dict):
            bigmap[counter] = {
                "key": bigmap_key["key"],
                "value": bigmap_key["value"]}
            counter += 1
        else:
            bigmap[bigmap_key["key"]] = bigmap_key["value"]

    return bigmap


def extract_artist_accounts(transactions, registries_bigmap, wallets):
    """Extracts the artists accounts information from a list of mint
    transactions.

    Parameters
    ----------
    transactions: list
        The list of mint transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the unique artists accounts.

    """
    artists = {}
    counter = 1

    for transaction in transactions:
        address = transaction["initiator"]["address"]
        objkt_id = transaction["parameter"]["value"]["token_id"]

        if address.startswith("tz"):
            if address not in artists:
                # Get the artist alias
                if address in registries_bigmap:
                    alias = registries_bigmap[address]["user"]
                elif "alias" in wallets[address]:
                    alias = wallets[address]["alias"]
                else:
                    alias = ""

                # Add the artist information
                artists[address] = {
                    "order": counter,
                    "type": "artist",
                    "address": address,
                    "alias": alias,
                    "reported": False,
                    "first_objkt": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "last_objkt": {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "mint",
                        "timestamp": transaction["timestamp"]},
                    "minted_objkts": [objkt_id],
                    "money_spent": [],
                    "total_money_spent": 0}

                counter += 1
            else:
                # Update the last minted OBJKT information
                artists[address]["last_objkt"] = {
                        "id": objkt_id,
                        "timestamp": transaction["timestamp"]}

                # Add the OBJKT id to the minted OBJKTs list
                artists[address]["minted_objkts"].append(objkt_id)

    return artists


def extract_collector_accounts(transactions, registries_bigmap, swaps_bigmap,
                               tezos_wallets):
    """Extracts the collector accounts information from a list of collect
    transactions.

    Parameters
    ----------
    transactions: list
        The list of collect transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    swaps_bigmap: dict
        The H=N marketplace swaps bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the unique collector accounts.

    """
    collectors = {}
    counter = 1

    for transaction in transactions:
        wallet_id = transaction["sender"]["address"]

        if wallet_id.startswith("tz"):
            if wallet_id not in collectors:
                # Get the collector alias
                if wallet_id in registries_bigmap:
                    alias = registries_bigmap[wallet_id]["user"]
                elif "alias" in tezos_wallets[wallet_id]:
                    alias = tezos_wallets[wallet_id]["alias"]
                else:
                    alias = ""

                # Get the swap id from the collect entrypoint input parameters
                parameters = transaction["parameter"]["value"]

                if isinstance(parameters, dict):
                    swap_id = parameters["swap_id"]
                else:
                    swap_id = parameters

                # Add the collector information
                collectors[wallet_id] = {
                    "order": counter,
                    "type": "collector",
                    "wallet_id": wallet_id,
                    "alias": alias,
                    "reported": False,
                    "first_collect": {
                        "objkt_id": swaps_bigmap[swap_id]["objkt_id"],
                        "timestamp": transaction["timestamp"]},
                    "last_collect": {
                        "objkt_id": swaps_bigmap[swap_id]["objkt_id"],
                        "timestamp": transaction["timestamp"]},
                    "first_interaction": {
                        "type": "collect",
                        "timestamp": transaction["timestamp"]},
                    "money_spent": [transaction["amount"] / 1e6]}
                counter += 1
            else:
                # Update the last collect information
                collectors[wallet_id]["last_collect"]["id"] = swaps_bigmap[swap_id]["objkt_id"]
                collectors[wallet_id]["last_collect"]["timestamp"] = transaction["timestamp"]

                # Add the money spent
                collectors[wallet_id]["money_spent"].append(
                    transaction["amount"] / 1e6)

    for collector in collectors.values():
        collector["total_money_spent"] = sum(collector["money_spent"])

    return collectors


def extract_swapper_accounts(transactions, registries_bigmap, tezos_wallets):
    """Extracts the swapper accounts information from a list of swap
    transactions.

    Parameters
    ----------
    transactions: list
        The list of swap transactions.
    registries_bigmap: dict
        The H=N registries bigmap.
    tezos_wallets: dict
        The complete list of tezos wallets.

    Returns
    -------
    dict
        A python dictionary with the unique swapper accounts.

    """
    swappers = {}
    counter = 1

    for transaction in transactions:
        wallet_id = transaction["sender"]["address"]

        if wallet_id.startswith("tz"):
            if wallet_id not in swappers:
                # Get the swapper alias
                if wallet_id in registries_bigmap:
                    alias = registries_bigmap[wallet_id]["user"]
                elif "alias" in tezos_wallets[wallet_id]:
                    alias = tezos_wallets[wallet_id]["alias"]
                else:
                    alias = ""

                # Add the swapper information
                swappers[wallet_id] = {
                    "order": counter,
                    "type": "swapper",
                    "wallet_id": wallet_id,
                    "alias": alias,
                    "reported": False,
                    "first_interaction": {
                        "type": "swap",
                        "timestamp": transaction["timestamp"]},
                    "money_spent": [],
                    "total_money_spent": 0}
                counter += 1

    return swappers


def get_patron_accounts(artists, collectors):
    """Gets the patron accounts from a set of artists and collectors.

    Parameters
    ----------
    artists: dict
        The python dictionary with the artists accounts.
    collectors: dict
        The python dictionary with the collectors accounts.

    Returns
    -------
    dict
        A python dictionary with the patron accounts.

    """
    patrons = {}

    for wallet_id, collector in collectors.items():
        # Check if the collector is also an artist
        if wallet_id in artists:
            # Set the collector type to artist
            collector["type"] = "artist"

            # Save the first collect information and the money spent
            artist = artists[wallet_id]
            artist["first_collect"] = collector["first_collect"]
            artist["last_collect"] = collector["last_collect"]
            artist["money_spent"] = collector["money_spent"]
            artist["total_money_spent"] = collector["total_money_spent"]

            # Check which was the first artist interation
            first_objkt_date = get_datetime_from_timestamp(
                artist["first_objkt"]["timestamp"])
            first_collect_date = get_datetime_from_timestamp(
                artist["first_collect"]["timestamp"])

            if first_collect_date < first_objkt_date:
                artist["first_interaction"]["type"] = "collect"
                artist["first_interaction"]["timestamp"] = artist[
                    "first_collect"]["timestamp"]
        else:
            # Set the collector type to patron
            collector["type"] = "patron"

            # Add the collector to the patrons dictionary
            patrons[wallet_id] = collector

    return patrons


def get_user_accounts(artists, patrons, swappers):
    """Gets the user accounts from a set of artists, patrons and swappers.

    Parameters
    ----------
    artists: dict
        The python dictionary with the artists accounts.
    patrons: dict
        The python dictionary with the patrons accounts.
    swappers: dict
        The python dictionary with the swappers accounts.

    Returns
    -------
    dict
        A python dictionary with the user accounts.

    """
    # Get the only swappers accounts
    only_swappers = {}

    for wallet, swapper in swappers.items():
        if wallet not in artists and wallet not in patrons:
            only_swappers[wallet] = swapper

    # Get the combined wallet ids and time stamps
    wallet_ids = np.array(
        [wallet_id for wallet_id in artists] + 
        [wallet_id for wallet_id in patrons] + 
        [wallet_id for wallet_id in only_swappers])
    timestamps = np.array(
        [artist["first_interaction"]["timestamp"] for artist in artists.values()] + 
        [patron["first_interaction"]["timestamp"] for patron in patrons.values()] + 
        [swapper["first_interaction"]["timestamp"] for swapper in only_swappers.values()])

    # Order the users by their time stamps
    dates = np.array(
        [get_datetime_from_timestamp(timestamp) for timestamp in timestamps])
    wallet_ids = wallet_ids[np.argsort(dates)]
    users = {}

    for wallet_id in wallet_ids:
        if wallet_id in artists:
            users[wallet_id] = artists[wallet_id]
        elif wallet_id in patrons:
            users[wallet_id] = patrons[wallet_id]
        else:
            users[wallet_id] = only_swappers[wallet_id]

    return users


def get_objkt_creators(transactions):
    """Gets a dictionary with the OBJKT creators from a list of mint
    transactions.

    Parameters
    ----------
    transactions: list
        The list of mint transactions.

    Returns
    -------
    dict
        A python dictionary with the OBJKT creators.

    """
    objkt_creators = {}

    for transaction in transactions:
        objkt_id = transaction["parameter"]["value"]["token_id"]
        creator = transaction["initiator"]["address"]
        objkt_creators[objkt_id] = creator

    return objkt_creators


def extract_users_connections(objkt_creators, transactions, swaps_bigmap,
                              users, objktcom_collectors, reported_users):
    """Extracts the users connections.

    Parameters
    ----------
    objkt_creators: dict
        The OBJKT creators.
    transactions: list
        The list of collect transactions.
    swaps_bigmap: dict
        The H=N marketplace swaps bigmap.
    users: dict
        The H=N users.
    objktcom_collectors: dict
        The objkt.com collectors.
    reported_users: list
        The python list with the wallet ids of all H=N reported users.

    Returns
    -------
    tuple
        A python tuple with the users connections information and a serialized
        version of it.

    """
    users_connections = {}
    user_counter = 0

    for artist_wallet_id in objkt_creators.values():
        # Add the artists wallets, since it could be that they might not have
        # connections from collect operations
        if artist_wallet_id.startswith("KT"):
            continue
        elif artist_wallet_id not in users_connections:
            if artist_wallet_id in users:
                alias = users[artist_wallet_id]["alias"]
            else:
                alias = ""

            users_connections[artist_wallet_id] = {
                "alias": alias,
                "artists": {},
                "collectors": {},
                "reported": False,
                "counter": user_counter}
            user_counter += 1

    for transaction in transactions:
        # Get the swap id from the collect entrypoint input parameters
        parameters = transaction["parameter"]["value"]

        if isinstance(parameters, dict):
            swap_id = parameters["swap_id"]
        else:
            swap_id = parameters

        # Get the objkt id from the swaps bigmap
        objkt_id = swaps_bigmap[swap_id]["objkt_id"]

        # Get the collector and artist wallet ids
        collector_wallet_id = transaction["sender"]["address"]
        artist_wallet_id = objkt_creators[objkt_id]

        # Move to the next transaction if one of the walles is a contract
        if (artist_wallet_id.startswith("KT") or 
            collector_wallet_id.startswith("KT")):
            continue

        # Move to the next transaction if the artist and the collector coincide
        if artist_wallet_id == collector_wallet_id:
            continue

        # Add the collector to the artist collectors list
        collectors = users_connections[artist_wallet_id]["collectors"]

        if collector_wallet_id in collectors:
            collectors[collector_wallet_id] += 1
        else:
            collectors[collector_wallet_id] = 1

        # Add the artist to the collector artists list
        if collector_wallet_id in users_connections:
            artists = users_connections[collector_wallet_id]["artists"]

            if artist_wallet_id in artists:
                artists[artist_wallet_id] += 1
            else:
                artists[artist_wallet_id] = 1
        else:
            if collector_wallet_id in users:
                alias = users[collector_wallet_id]["alias"]
            else:
                alias = ""

            users_connections[collector_wallet_id] = {
                "alias": alias,
                "artists": {artist_wallet_id: 1},
                "collectors": {},
                "reported": False,
                "counter": user_counter}
            user_counter += 1

    for collector_wallet_id, objktcom_collector in objktcom_collectors.items():
        # Get the objkt ids
        objkt_ids = (objktcom_collector["bid_objkts"] + 
                     objktcom_collector["ask_objkts"] + 
                     objktcom_collector["english_auction_objkts"] + 
                     objktcom_collector["dutch_auction_objkts"])

        # Loop over the collected OBJKT ids
        for objkt_id in objkt_ids:
            # Get the artist wallet id
            artist_wallet_id = objkt_creators[objkt_id]

            # Move to the next OBJKT if the wallet is a contract
            if artist_wallet_id.startswith("KT"):
                continue

            # Move to the next OBJKT if the artist and the collector coincide
            if artist_wallet_id == collector_wallet_id:
                continue

            # Add the collector to the artist collectors list
            collectors = users_connections[artist_wallet_id]["collectors"]

            if collector_wallet_id in collectors:
                collectors[collector_wallet_id] += 1
            else:
                collectors[collector_wallet_id] = 1

            # Add the artist to the collector artists list
            if collector_wallet_id in users_connections:
                artists = users_connections[collector_wallet_id]["artists"]

                if artist_wallet_id in artists:
                    artists[artist_wallet_id] += 1
                else:
                    artists[artist_wallet_id] = 1
            else:
                if collector_wallet_id in users:
                    alias = users[collector_wallet_id]["alias"]
                else:
                    alias = objktcom_collector["alias"]

                users_connections[collector_wallet_id] = {
                    "alias": alias,
                    "artists": {artist_wallet_id: 1},
                    "collectors": {},
                    "reported": False,
                    "counter": user_counter}
                user_counter += 1

    # Fill the reported user information
    for reported_user_wallet_id in reported_users:
        if reported_user_wallet_id in users_connections:
            users_connections[reported_user_wallet_id]["reported"] = True

    # Process the connections information to a different format
    for user in users_connections.values():
        # Get the lists of artist and collectors wallets
        artists_and_collectors_wallets = [
            wallet for wallet in user["artists"] if wallet in user["collectors"]]
        artists_wallets = [
            wallet for wallet in user["artists"] if not wallet in user["collectors"]]
        collectors_wallets = [
            wallet for wallet in user["collectors"] if not wallet in user["artists"]]

        # Get the lists of artists and collectors weights
        artists_and_collectors_weights = [
            user["artists"][wallet] + user["collectors"][wallet] for wallet in artists_and_collectors_wallets]
        artists_weights = [
            user["artists"][wallet] for wallet in artists_wallets]
        collectors_weights = [
            user["collectors"][wallet] for wallet in collectors_wallets]

        # Add these lists to the user information
        user["artists_and_collectors"] = artists_and_collectors_wallets
        user["artists_and_collectors_weights"] = artists_and_collectors_weights
        user["artists"] = artists_wallets
        user["artists_weights"] = artists_weights
        user["collectors"] = collectors_wallets
        user["collectors_weights"] = collectors_weights

    # Create the serialized users connections
    serialized_users_connections = {}

    for wallet_id, user in users_connections.items():
        serialized_artists_and_collectors = [
            users_connections[artist_and_collector]["counter"] for artist_and_collector in user["artists_and_collectors"]]
        serialized_artists = [
            users_connections[artist]["counter"] for artist in user["artists"]]
        serialized_collectors = [
            users_connections[collector]["counter"] for collector in user["collectors"]]

        serialized_users_connections[user["counter"]] = {
            "wallet": wallet_id,
            "alias": user["alias"],
            "artists_and_collectors": serialized_artists_and_collectors,
            "artists_and_collectors_weights": user["artists_and_collectors_weights"],
            "artists": serialized_artists,
            "artists_weights": user["artists_weights"],
            "collectors": serialized_collectors,
            "collectors_weights": user["collectors_weights"],
            "reported": user["reported"]}

    return users_connections, serialized_users_connections


def add_reported_users_information(accounts, reported_users):
    """Adds the reported users information to a set of accounts.

    Parameters
    ----------
    accounts: dict
        The python dictionary with the accounts information.
    reported_users: list
        The python list with the wallet ids of all H=N reported users.

    """
    for wallet_id in reported_users:
        if wallet_id in accounts:
            accounts[wallet_id]["reported"] = True


def group_users_per_day(users, first_year=2021, first_month=3, first_day=1):
    """Groups the given users per the day of their first interaction.

    Parameters
    ----------
    users: dict
        A python dictionary with the users information.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.

    Returns
    -------
    list
        A python list with the users grouped by day.

    """
    # Get the users wallet ids and their first interation time stamp
    wallet_ids = np.array(list(users.keys()))
    timestamps = np.array(
        [user["first_interaction"]["timestamp"] for user in users.values()])

    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the users per day
    users_per_day = []
    started = False
    finished = False
    now = datetime.utcnow()

    for year in range(first_year, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the starting day
                if not started:
                    started = ((year == first_year) and 
                               (month == first_month) and 
                               (day == first_day))

                # Check that we started and didn't finish yet
                if started and not finished:
                    selected_wallets_ids = wallet_ids[
                        (years == year) & (months == month) & (days == day)]
                    users_per_day.append(
                        [users[wallet_id] for wallet_id in selected_wallets_ids])

                    # Check if we reached the current day
                    finished = (year == now.year) and (
                        month == now.month) and (day == now.day)

    return users_per_day
