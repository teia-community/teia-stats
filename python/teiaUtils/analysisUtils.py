import json
import numpy as np
from datetime import datetime
from calendar import monthrange


def print_info(info):
    """Prints some information with a time stamp added.

    Parameters
    ----------
    info: str
        The information to print.

    """
    print("%s  %s" % (datetime.utcnow(), info))


def read_json_file(file_name):
    """Reads a json file from disk.

    Parameters
    ----------
    file_name: str
        The complete path to the json file.

    Returns
    -------
    object
        The content of the json file.

    """
    with open(file_name, "r", encoding="utf-8") as json_file:
        return json.load(json_file)


def save_json_file(file_name, data, compact=False):
    """Saves some data as a json file.

    Parameters
    ----------
    file_name: str
        The complete path to the json file where the data will be saved.
    data: object
        The data to save.
    compact: bool, optional
        If True, the json file will be saved in a compact form. Default is
        False.

    """
    with open(file_name, "w", encoding="utf-8") as json_file:
        if compact:
            json.dump(data, json_file, indent=None, separators=(",", ":"))
        else:
            json.dump(data, json_file, indent=4)


def hex_to_utf8(hex_string):
    """Transforms a hex string to a utf-8 string.

    Parameters
    ----------
    hex_string: str
        The hex string.

    Returns
    -------
    str
        The utf-8 string.

    """
    return bytes.fromhex(hex_string).decode("utf-8", errors="replace")


def select_keys(dictionary, keys):
    """Selects a list of keys from an input dictionary.

    Parameters
    ----------
    dictionary: dict
        The python dictionary.
    keys: list
        The list of keys to select.

    Returns
    -------
    dict
        A python dictionary with the selected keys.

    """
    return {key: dictionary[key] for key in keys if key in dictionary}


def get_datetime_from_timestamp(timestamp):
    """Returns a datetime instance from a time stamp.

    Parameters
    ----------
    timestamp: str
        The time stamp.

    Returns
    -------
    object
        A datetime instance.

    """
    return datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")


def split_timestamps(timestamps):
    """Splits the input time stamps in 3 arrays containing the years, months
    and days.

    Parameters
    ----------
    timestamps: list
        A python list with the time stamps.

    Returns
    -------
    tuple
        A python tuple with the years, months and days numpy arrays.

    """
    years = np.empty(len(timestamps), dtype=int)
    months = np.empty(len(timestamps), dtype=int)
    days = np.empty(len(timestamps), dtype=int)

    for i, timestamp in enumerate(timestamps):
        years[i] = timestamp.split("-")[0]
        months[i] = timestamp.split("-")[1]
        days[i] = timestamp.split("-")[2].split("T")[0]

    return years, months, days


def get_counts_per_day(timestamps, first_year=2021, first_month=3, first_day=1):
    """Calculates the counts per day for a list of time stamps.

    Parameters
    ----------
    timestamps: list
        A python list with the time stamps.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.

    Returns
    -------
    list
        A python list with the counts per day, starting from the given first
        date.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the counts per day
    counts_per_day = []
    started = False
    finished = False
    now = datetime.utcnow()

    for year in range(first_year, np.max(years) + 1):
        for month in range(1, 13):
            for day in range(1, monthrange(year, month)[1] + 1):
                # Check if we passed the first day
                if not started:
                    started = ((year == first_year) and 
                               (month == first_month) and 
                               (day == first_day))

                # Check that we started and didn't finish yet
                if started and not finished:
                    # Add the number of counts for the current day
                    counts_per_day.append(np.sum(
                        (years == year) & (months == month) & (days == day)))

                    # Check if we reached the current day
                    finished = ((year == now.year) and
                                (month == now.month) and
                                (day == now.day))

    return counts_per_day


def group_users_per_day(users, first_year=2021, first_month=3, first_day=1):
    """Groups a set of users by the day of their first activity.

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
    # Get the users addresses and their first activity time stamp
    addresses = np.array(list(users.keys()))
    timestamps = np.array(
        [user.first_activity["timestamp"] for user in users.values()])

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
                    # Add the users that were first active that day
                    selected_addresses = addresses[
                        (years == year) & (months == month) & (days == day)]
                    users_per_day.append(
                        [users[address] for address in selected_addresses])

                    # Check if we reached the current day
                    finished = ((year == now.year) and
                                (month == now.month) and
                                (day == now.day))

    return users_per_day


def get_objkt_creators(transactions):
    """Returns a dictionary with the OBJKT creators from a list of mint
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
        address = transaction["initiator"]["address"]
        objkt_creators[objkt_id] = address

    return objkt_creators
