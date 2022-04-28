import numpy as np
import matplotlib.pyplot as plt
from calendar import monthrange
from datetime import datetime
from datetime import timezone

from teiaUtils.analysisUtils import get_counts_per_day
from teiaUtils.analysisUtils import split_timestamps
from teiaUtils.queryUtils import get_tez_exchange_rates


def plot_histogram(data, title, x_label, y_label, bins=100, log=False, **kwargs):
    """Plots a histogram of the given data.

    Parameters
    ----------
    data: object
        A numpy array with the data to use for the histogram.
    title: str
        The histogram title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    bins: int, optional
        The number of bins that the histogram should have. Default is 100.
    log: bool, optional
        If true the y axis will be in log scale. Default is False.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.hist(data, bins=bins, log=log)
    plt.show(block=False)


def plot_transactions_per_day(transactions, title, x_label, y_label,
                              exclude_last_day=False, first_year=2021,
                              first_month=3, first_day=1, **kwargs):
    """Plots the number of transactions per day as a function of time.

    Parameters
    ----------
    transactions: list
        A python list with the transactions information.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Get the transactions per day
    timestamps = [transaction["timestamp"] for transaction in transactions]
    transactions_per_day = get_counts_per_day(
        timestamps, first_year=first_year, first_month=first_month,
        first_day=first_day)

    if exclude_last_day:
        transactions_per_day = transactions_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.ylim(-0.05 * max(transactions_per_day), 1.05 * max(transactions_per_day))
    plt.plot(transactions_per_day)
    plt.show(block=False)


def plot_new_users_per_day(users, title, x_label, y_label,
                           exclude_last_day=False, first_year=2021,
                           first_month=3, first_day=1, **kwargs):
    """Plots the new users per day as a function of time.

    Parameters
    ----------
    users: TeiaUsers
        A TeiaUsers instance with the users information.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Get the users per day
    timestamps = [user.first_activity["timestamp"] for user in users.values()]
    users_per_day = get_counts_per_day(
        timestamps, first_year=first_year, first_month=first_month,
        first_day=first_day)

    if exclude_last_day:
        users_per_day = users_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(users_per_day)
    plt.show(block=False)


def plot_data_per_day(data, timestamps, title, x_label, y_label,
                      exclude_last_day=False, first_year=2021, first_month=3,
                      first_day=1, add_exchange_rates=False,
                      exchange_rates_scaling=1, **kwargs):
    """Plots some combined data per day as a function of time.

    Parameters
    ----------
    data: object
        A numpy array with the data.
    timestamps: object
        A numpy array with the timestamps.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.
    add_exchange_rates: bool, optional
        If True the tez to USD exchange rates will be added. Default is False.
    exchange_rates_scaling: float, optional
        The scaling to apply to the exchange rates values. Default is 1.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the data per day
    data = np.array(data)
    data_per_day = []
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
                    # Add the combined data for the current day
                    data_per_day.append(np.sum(data[
                        (years == year) & (months == month) & (days == day)]))

                    # Check if we reached the current day
                    finished = ((year == now.year) and
                                (month == now.month) and
                                (day == now.day))

    if exclude_last_day:
        data_per_day = data_per_day[:-1]

    # Get the tez exchange rates if necessary
    if add_exchange_rates:
        datetime_format = "%Y-%m-%dT%H:%M:%SZ"
        start_date = datetime(
            first_year, first_month, first_day, tzinfo=timezone.utc)
        _, exchange_rates = get_tez_exchange_rates(
            "USD", start_date=start_date.strftime(datetime_format),
            end_date=None, sampling="1d")

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.ylim(min(min(data_per_day), -0.05 * max(data_per_day)),
             1.05 * max(data_per_day))
    plt.plot(data_per_day)

    if add_exchange_rates:
        plt.plot(exchange_rates_scaling * np.array(exchange_rates))

    plt.show(block=False)


def plot_price_distribution_per_day(money, timestamps, price_ranges, title,
                                    x_label, y_label, exclude_last_day=False,
                                    first_year=2021, first_month=3, first_day=1,
                                    **kwargs):
    """Plots the price distribution in collect transaction per day as a function
    of time.

    Parameters
    ----------
    money: object
        A numpy array with the money of each collect transaction.
    timestamps: object
        A numpy array with the timestamps of each collect transaction.
    price_ranges: list
        A python list with 4 elements indicating the price ranges to use.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the operation counts in the different price ranges per day
    counts_range_1 = []
    counts_range_2 = []
    counts_range_3 = []
    counts_range_4 = []
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
                    # Get the number of operations for the current day in each range
                    cond = (years == year) & (months == month) & (days == day)
                    counts_range_1.append(np.sum(
                        (money[cond] >= price_ranges[0]) & (money[cond] < price_ranges[1])))
                    counts_range_2.append(np.sum(
                        (money[cond] >= price_ranges[1]) & (money[cond] < price_ranges[2])))
                    counts_range_3.append(np.sum(
                        (money[cond] >= price_ranges[2]) & (money[cond] < price_ranges[3])))
                    counts_range_4.append(10 * np.sum(
                        (money[cond] >= price_ranges[3])))

                    # Check if we reached the last day
                    finished = ((year == now.year) and
                                (month == now.month) and
                                (day == now.day))

    if exclude_last_day:
        counts_range_1 = counts_range_1[:-1]
        counts_range_2 = counts_range_2[:-1]
        counts_range_3 = counts_range_3[:-1]
        counts_range_4 = counts_range_4[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(counts_range_1, label="%.2f tez <= edition price < %.0f tez" % (
        price_ranges[0], price_ranges[1]))
    plt.plot(counts_range_2, label="%.0f tez <= edition price < %.0f tez" % (
        price_ranges[1], price_ranges[2]))
    plt.plot(counts_range_3, label="%.0f tez <= edition price < %.0f tez" % (
        price_ranges[2], price_ranges[3]))
    plt.plot(counts_range_4, label="edition price <= %.0f tez (x10)" % (
        price_ranges[3]))
    plt.legend()
    plt.show(block=False)


def plot_active_users_per_day(addresses, timestamps, users, title, x_label,
                              y_label, exclude_last_day=False, first_year=2021,
                              first_month=3, first_day=1, **kwargs):
    """Plots the active users per day as a function of time.

    Parameters
    ----------
    addresses: object
        A numpy array with the address associated to each transaction.
    timestamps: object
        A numpy array with the timestamps of each transaction.
    users: TeiaUsers
        A TeiaUsers instance with the users information.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Extract the years, months and days from the time stamps
    years, months, days = split_timestamps(timestamps)

    # Get the active users, artists and patrons per day
    active_users_per_day = []
    active_artists_per_day = []
    active_patrons_per_day = []
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
                    # Get the number of unique users for the current day
                    unique_users = np.unique(addresses[
                        (years == year) & (months == month) & (days == day)])
                    #is_artist = np.array([
                    #    (address in users and users[address].type == "artist") 
                    #    for address in unique_users])
                    #is_patron = np.array([
                    #    (address in users and users[address].type == "patron") 
                    #    for address in unique_users])
                    active_users_per_day.append(len(unique_users))
                    #active_artists_per_day.append(np.sum(is_artist))
                    #active_patrons_per_day.append(np.sum(is_patron))

                    # Check if we reached the last day
                    finished = ((year == now.year) and
                                (month == now.month) and
                                (day == now.day))

    if exclude_last_day:
        active_users_per_day = active_users_per_day[:-1]
        #active_artists_per_day = active_artists_per_day[:-1]
        #active_patrons_per_day = active_patrons_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(active_users_per_day, label="all users")
    #plt.plot(active_artists_per_day, label="artists")
    #plt.plot(active_patrons_per_day, label="patrons")
    plt.legend()
    plt.show(block=False)


def plot_users_last_active_day(addresses, timestamps, title, x_label, y_label,
                               exclude_last_day=False, first_year=2021,
                               first_month=3, first_day=1, **kwargs):
    """Plots users last active day as a function of time.

    Parameters
    ----------
    addresses: object
        A numpy array with the address associated to each transaction.
    timestamps: object
        A numpy array with the timestamps of each transaction.
    title: str
        The plot title.
    x_label: str
        The label for the x axis.
    y_label: str
        The label for the y axis.
    exclude_last_day: bool, optional
        If True the last day will be excluded from the plot. Default is False.
    first_year: int, optional
        The first year to count. Default is 2021.
    first_month: int, optional
        The first month to count. Default is 3 (March).
    first_day: int, optional
        The first day to count. Default is 1.
    kwargs: plt.figure properties
        Any additional property that should be passed to the figure.

    """
    # Get the users last activity time stamp
    users_last_activity = {}

    for address, timestamp in zip(addresses, timestamps):
        if address in users_last_activity:
            if users_last_activity[address] < timestamp:
                users_last_activity[address] = timestamp
        else:
            users_last_activity[address] = timestamp

    # Get the last activity time stamps
    timestamps = list(users_last_activity.values())

    # Get the users per day
    users_per_day = get_counts_per_day(
        timestamps, first_year=first_year, first_month=first_month,
        first_day=first_day)

    if exclude_last_day:
        users_per_day = users_per_day[:-1]

    # Create the figure
    plt.figure(figsize=(7, 5), facecolor="white", tight_layout=True, **kwargs)
    plt.title(title)
    plt.xlabel(x_label)
    plt.ylabel(y_label)
    plt.plot(users_per_day)
    plt.show(block=False)


def save_figure(file_name, **kwargs):
    """Saves an image of the current figure.

    Parameters
    ----------
    file_name: object
        The complete path to the file where the figure should be saved.
    kwargs: figure.savefig properties
        Any additional property that should be passed to the savefig method.

    """
    plt.gcf().savefig(file_name, **kwargs)
