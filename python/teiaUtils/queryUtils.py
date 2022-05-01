import json
import requests
import time
import os.path
from datetime import datetime, timezone

import teiaUtils.analysisUtils as utils


def get_query_result(url, parameters=None, timeout=10):
    """Executes the given query and returns the result.

    Parameters
    ----------
    url: str
        The url to the server API.
    parameters: dict, optional
        The query parameters. Default is None.
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


def get_restricted_addresses():
    """Returns the list of restricted addresses stored in the Teia Community
    github repository.

    Returns
    -------
    list
        A python list with the restricted addresses.

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


def get_tzprofiles(batch_size=10000, sleep_time=1):
    """Returns the complete list of tzprofiles ordered by their wallet.

    Parameters
    ----------
    batch_size: int, optional
        The maximum number of tzprofiles per API query. Default is 10000. The
        maximum allowed by the API is 10000.
    sleep_time: float, optional
        The sleep time between API queries in seconds. This is used to avoid
        being blocked by the server. Default is 1 second.

    Returns
    -------
    dict
        A python dictionary with the tzprofiles information.

    """
    utils.print_info("Downloading the complete list of tzprofiles...")
    tzprofiles = []
    counter = 0

    while True:
        utils.print_info("Downloading batch %i" % (counter + 1))
        url = "https://unstable-do-not-use-in-production-api.teztok.com/v1/graphql"
        graphql_query = """query TzProfiles {
            tzprofiles(distinct_on: account, order_by: {}, limit: %i, offset: %i) {
                alias
                description
                discord
                domain_name
                ethereum
                github
                logo
                twitter
                website
                contract
                account
            }
        }""" % (batch_size, counter * batch_size)
        result = get_graphql_query_result(url, {"query": graphql_query})
        new_tzprofiles = result["data"]["tzprofiles"]
        tzprofiles += new_tzprofiles

        if len(new_tzprofiles) != batch_size:
            break

        time.sleep(sleep_time)
        counter += 1

    utils.print_info("Downloaded %i tzprofiles." % len(tzprofiles))

    return {tzprofile["account"]: tzprofile for tzprofile in tzprofiles}


def get_transactions(entrypoint, contract, offset=0, limit=10000,
                     timestamp=None, extra_parameters=None):
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
        Extra parameters to apply to the query. Default is None.

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

    if extra_parameters is not None:
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
            if name == "ledger" and token in ["hDAO", "MATERIA"]:
                bigmap[bigmap_key["key"]["address"]] = bigmap_key["value"]
            else:
                bigmap[counter] = {
                    "key": bigmap_key["key"],
                    "value": bigmap_key["value"]}
                counter += 1
        else:
            bigmap[bigmap_key["key"]] = bigmap_key["value"]

    return bigmap


def get_artists_collaborations(offset=0, limit=10000):
    """Returns a dictionary with all the artists collaborations originations
    information.

    Parameters
    ----------
    offset: int, optional
        The number of initial originations that should be skipped. This is
        mostly used for pagination. Default is 0.
    limit: int, optional
        The maximum number of originations to return. Default is 10000. The
        maximum allowed by the API is 10000.

    Returns
    -------
    dict
        A python dictionary with the artists collaborations originations
        information.

    """
    # Get the originations from the artists collaborations
    url = "https://api.tzkt.io/v1/operations/originations"
    parameters = {
        "sender": "KT1DoyD6kr8yLK8mRBFusyKYJUk2ZxNHKP1N",
        "status": "applied",
        "offset": offset,
        "limit": limit,
        "select": "timestamp,initiator,originatedContract,storage"
    }
    originations = get_query_result(url, parameters)

    # Build the dictionary with the artists collaboration information
    artists_collaborations = {}

    for origination in originations:
        artists_collaborations[
            origination["originatedContract"]["address"]] = origination

    return artists_collaborations


def get_artists_collaborations_signatures(offset=0, limit=10000):
    """Returns a dictionary with all the artists collaborations signatures
    information.

    Parameters
    ----------
    offset: int, optional
        The number of initial signatures that should be skipped. This is mostly
        used for pagination. Default is 0.
    limit: int, optional
        The maximum number of signatures to return. Default is 10000. The
        maximum allowed by the API is 10000.

    Returns
    -------
    dict
        A python dictionary with the artists collaborations signatures
        information.

    """
    # Get the artists collaborations signatures
    url = "https://api.tzkt.io/v1/operations/transactions"
    parameters = {
        "target": "KT1BcLnWRziLDNJNRn3phAANKrEBiXhytsMY",
        "entrypoint": "sign",
        "status": "applied",
        "offset": offset,
        "limit": limit
    }
    signatures = get_query_result(url, parameters)

    # Build the dictionary with the artists collaborations signatures
    artists_collaborations_signatures = {}

    for signature in signatures:
        address = signature["sender"]["address"]
        objkt_id = signature["parameter"]["value"]

        if address in artists_collaborations_signatures:
            artists_collaborations_signatures[address].append(objkt_id)
        else:
            artists_collaborations_signatures[address] = [objkt_id]

    return artists_collaborations_signatures


def get_teia_community_votes():
    """Returns the votes from the Teia Community vote contract.

    Returns
    -------
    dict
        A python dictionary with the Teia Community votes.

    """
    # Get the votes bigmap keys
    url = "https://api.tzkt.io/v1/bigmaps/64367/keys"
    parameters = {"limit": 10000}
    bigmap_keys = get_query_result(url, parameters)

    # Build the dictionary with the votes
    votes = {}

    for bigmap_key in bigmap_keys:
        address = bigmap_key["key"]["address"]
        poll = bigmap_key["key"]["string"]
        vote = bigmap_key["value"]

        if address not in votes:
            votes[address] = {}

        votes[address][poll] = vote

    return votes
