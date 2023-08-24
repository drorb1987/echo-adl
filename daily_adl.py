import pandas as pd
import numpy as np
import datetime
from collections import Counter, defaultdict
from typing import Dict, Tuple

AWAKE = 0
SLEEP = 1

HOURS_PER_DAY = 24
SPLITS_PER_HOUR = 2
NUM_DAYS = 2

MAX_AWAKE = 3 # maximum awake time between sleep sessions


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Validate dataframe, chage to timestamp format and sort.

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        pd.DataFrame: a dataframe that ready for further uses
    """
    for t in ['start', 'stop', 'time']:
        if t in df:
            for d in df[t]:
                if str(d)[-1] == 'Z':
                    df[t] = pd.to_datetime(df[t]).dt.tz_localize(None)
                else:
                    df[t] = pd.to_datetime(df[t])
            if t != 'stop':
                df = df.sort_values(t, ignore_index=True)
    return df


def create_consecutive_df(df: pd.DataFrame) -> pd.DataFrame:
    """Create consecutive dataframe

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        pd.DataFrame: a dataframe that gets the consecutive time
    """
    # remove the non-string handling
    df = validate_df(df)
    name = df.columns[2]
    is_object = df[name].dtype == object
    start_index = stop_index = 0
    starts = []
    stops = []
    values = []
    for i in range(1, len(df)):
        cond = (df.loc[i, 'start'] - df.loc[stop_index, 'stop'] < pd.Timedelta(minutes=1))
        if is_object:
            cond = cond and df.loc[i, name] == df.loc[stop_index, name]
        if cond:
            stop_index = i
        else:
            starts.append(df.loc[start_index, 'start'])
            stops.append(df.loc[stop_index, 'stop'])
            if is_object:
                values.append(df.loc[stop_index, name])
            else:
                value = df.loc[start_index: stop_index+1, name].mean()
                values.append(value)
            start_index = stop_index = i
        if i == len(df) - 1:
            starts.append(df.loc[start_index, 'start'])
            stops.append(df.loc[stop_index, 'stop'])
            if is_object:
                values.append(df.loc[stop_index, name])
            else:
                value = df.loc[start_index: stop_index+1, name].mean()
                values.append(value)
    return pd.DataFrame({'start': starts, 'stop': stops, name: values})


def count_out_of_bed_loaction_sleep(sleep_df: pd.DataFrame,
                                    location_df: pd.DataFrame,
                                    time_dict: dict,
                                    day_or_night: str) -> Tuple[int, defaultdict]:
    """Count the number of out of bed (location/sleep)

    Args:
        sleep_df (pd.DataFrame): a sleep dataframe
        location_df (pd.DataFrame): a location dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        tuple[int, defaultdict]: number of out of bed and counter dictionary for locations
    """
    rel_sleep_df = get_relevant_df(sleep_df, time_dict, day_or_night)
    rel_location_df = get_relevant_df(location_df, time_dict, day_or_night)
    awake_df = pd.DataFrame(
        {
            'start': rel_sleep_df[:-1]['stop'].to_numpy(),
            'stop': rel_sleep_df[1:]['start'].to_numpy()
        }
    )
    number_out_of_bed = 0
    out_of_bed_duration = 0
    counter = defaultdict(int)
    for _, row in awake_df.iterrows():
        cond = (row['start'] <= rel_location_df['start']) & \
            (row['stop'] >= rel_location_df['stop']) & \
            (rel_location_df['location'] != 'Bed')
        if len(rel_location_df[cond]):
            number_out_of_bed += rel_location_df[cond]['location'].count()
            out_of_bed_duration += rel_location_df[cond]['total_time'].sum()
            for location in rel_location_df[cond]['location'].unique():
                counter[location] += int(sum(rel_location_df[cond]['location'] == location))
    return int(number_out_of_bed), float(out_of_bed_duration), counter


def making_hours_array(df: pd.DataFrame, dates: pd.DatetimeIndex) -> np.ndarray:
    """Make the hours array

    Args:
        df (pd.DataFrame): a sleep sessions dataframe
        dates (pd.DatetimeIndex): the dates

    Returns:
        np.ndarray: hours array with sleep/awake indicators
    """
    hours = np.full(NUM_DAYS * SPLITS_PER_HOUR * HOURS_PER_DAY, AWAKE)
    for _, row in df.iterrows():
        # start time index
        is_curr_date = pd.Timestamp(row['start'].date()) == dates[-1]
        h = row['start'].time().hour
        m = row['start'].time().minute
        start_index = is_curr_date * HOURS_PER_DAY * SPLITS_PER_HOUR + h * SPLITS_PER_HOUR + np.floor((m / 60) * SPLITS_PER_HOUR)
        start_index = int(max(start_index, 0))
        # stop time index
        is_curr_date = pd.Timestamp(row['stop'].date()) == dates[-1]
        h = row['stop'].time().hour
        m = row['stop'].time().minute
        stop_index = is_curr_date * HOURS_PER_DAY * SPLITS_PER_HOUR + h * SPLITS_PER_HOUR + np.ceil((m / 60) * SPLITS_PER_HOUR)
        stop_index = int(min(len(hours) - 1, stop_index) + 1)
        # assigning the sleep session time to the hours array
        hours[start_index: stop_index] = SLEEP
    return hours


def clustering_day_night(hours: pd.DatetimeIndex) -> Tuple[int, int]:
    """Clustering for the day/night

    Args:
        hours (pd.DatetimeIndex): an array for hours, each hour there is an indication for sleep/awake

    Returns:
        tuple[int, int]: indices for night start and end
    """
    # clustering for day and night
    mid = len(hours) // 2
    night_start = mid - 1
    night_end = mid + 1
    start_awake_indices = np.where(hours[:mid] == AWAKE)
    end_awake_indices = np.where(hours[mid:] == AWAKE)

    for night_start in start_awake_indices[0][::-1]:
        if np.all(hours[max(0, night_start - MAX_AWAKE): night_start+1] == AWAKE):
            break
    for night_end in end_awake_indices[0] + mid:
        if np.all(hours[night_end: min(night_end+MAX_AWAKE+1, len(hours) - 1)] == AWAKE):
            break
    return night_start+1, night_end


def find_closest_time(df: pd.DataFrame, start_time: pd.Timestamp, end_time: pd.Timestamp):
    start_idx = np.argmin(abs(df['start'] - start_time))
    end_idx = np.argmin(abs(df['stop'] - end_time))
    return df.loc[start_idx, 'start'], df.loc[end_idx, 'stop']


def day_night_update_df(df: pd.DataFrame, time_dict: dict) -> pd.DataFrame:
    """Update the dataframe by adding a column for day/night

    Args:
        df (pd.DataFrame): a dataframe
        time_dict (dict): a dictionary for the day night times

    Returns:
        pd.DataFrame: a dataframe that contains the day/night column
    """
    if 'day_or_night' in df or not len(df):
        return df
    df['day_or_night'] = [None] * len(df)
    start = 'start' if 'start' in df else 'time'
    stop = 'stop' if 'stop' in df else 'time'
    for day_or_night in time_dict:
        rel_time = (df[start] >= time_dict[day_or_night]['start']) & (df[stop] <= time_dict[day_or_night]['end'])
        df.loc[rel_time, 'day_or_night'] = day_or_night
    if 'start' in df:
        df['total_time'] = (df['stop'] - df['start']).astype('timedelta64[s]') / (60.0 * pd.Timedelta(minutes=1))
    return df


def get_day_night_times(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Dict[str, pd.Timestamp]]]:
    """Get the day and night times from the sleep sessions data frame

    Args:
        df (pd.DataFrame): The sleep sessions data frame 

    Returns:
        tuple[dict[str, pd.Timestamp]]: day_time, night_time each have 'start' and 'end' timestamp
    """

    df = validate_df(df)

    # get the dates from the dataframe and check that there is 1 or 2 dates
    dates = pd.to_datetime(pd.concat([df['start'], df['stop']]).dt.date.unique()).sort_values()
    if len(dates) > 2:
        # dates = dates[-2:]
        dates = dates[:2]
    assert len(dates) <= 2, "There is more than 2 days"

    hours = making_hours_array(df, dates)

    night_start_idx, night_end_idx = clustering_day_night(hours)

    # need to check if the one date case can be removed (one date is only for validation)
    date_start = dates[0] if len(dates) == 2 else dates[0] - datetime.timedelta(days=1)
    date_end = dates[1] + datetime.timedelta(days=1) if len(dates) == 2 else dates[0] + datetime.timedelta(days=1)

    datetime_range = pd.date_range(start=date_start, periods=len(hours) + 1, end=date_end)
     
    day_delta = (0, 1) if df.iloc[-1]['stop'].time() > datetime.time(12, 0, 0) else (-1, 0)

    # find the closest datetime to night start and night end from the sleep sessions
    night_start, night_end = find_closest_time(df, datetime_range[night_start_idx], datetime_range[night_end_idx])

    night_time = {
        'start': night_start,
        'end': night_end
    }

    day_time = {
        'start': night_end + datetime.timedelta(days=day_delta[0]),
        'end': night_start + datetime.timedelta(days=day_delta[1])
    }

    time_dict  = {
        'Day': day_time,
        'Night': night_time
    }

    df = day_night_update_df(df, time_dict)

    return df, time_dict


def get_relevant_df(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> pd.DataFrame:
    """Get the relevant dataframe day/night time

    Args:
        df (pd.DataFrame): sleep sessoions data frame
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        pd.DataFrame: the relevant day/night dataframe
    """
    assert day_or_night in ['Day', 'Night'], "day_or_night can get values 'Day' or 'Night'"
    if not len(df):
        return df
    df = validate_df(df)
    df = day_night_update_df(df, time_dict)
    return df[df['day_or_night'] == day_or_night]


def get_sleep_duration(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get the sleep duration on day/night

    Args:
        df (pd.DataFrame): sleep sessoions data frame
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: The sleep duration in hours
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None
    # casting the value to float for writing the value to the api with the correct type
    return float(rel_df['total_time'].sum())


def get_restlessness(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get restlessness during sleep per day/night

    Args:
        df (pd.DataFrame): sleep sessions data frame
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: average restlessness
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None
    # casting the value to float for writing the value to the api with the correct type
    return float((rel_df['restless'] * rel_df['total_time']).sum() / rel_df['total_time'].sum())


def get_out_of_bed_number(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> Tuple[int, float]:
    """Get the number of out of bed and the duration

    Args:
        df (pd.DataFrame): location dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        tuple[int, float]: returns the number of out of bed times and the duration in hours
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None, None
    rel_df = rel_df[rel_df['location'] != 'Bed']
    # casting the value to float for writing the value to the api with the correct type
    return len(rel_df), float(rel_df['total_time'].sum())


def get_location_distribution(df: pd.DataFrame, time_dict: dict, day_or_night: str, bed_included: bool=False) -> Dict[str, float]:
    """Get location distribution during day/night

    Args:
        df (pd.DataFrame): location dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'
        bed_included (bool): flag if to include bed on not (default False)

    Returns:
        dict[str, float]: returns a dictionary for the locations distribution
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    location_dist = rel_df.groupby('location')['total_time'].sum().to_dict()
    if not bed_included and 'Bed' in location_dist:
        location_dist.pop('Bed')
    return location_dist


def get_average_respiration(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get average respiration during day/night

    Args:
        df (pd.DataFrame): respiration dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns an average respiration
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night).dropna()
    if not len(rel_df):
        return None
    # casting the value to float for writing the value to the api with the correct type
    return float(rel_df['respiration'].mean())


def get_average_heartrate(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get average heart-rate during day/night

    Args:
        df (pd.DataFrame): respiration dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns an average heart-rate
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night).dropna()
    rel_df = rel_df[rel_df['heart_rate'] != 0]
    if not len(rel_df):
        return None
    # casting the value to float for writing the value to the api with the correct type
    return float(rel_df['heart_rate'].mean())


def get_total_alone_time(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get total alone time during day/night

    Args:
        df (pd.DataFrame): visitors dataframe
        time_dict (dict): a dictionary for the start time and and time for day/night
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns total alone time
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    total_time_of_day = (time_dict[day_or_night]['end'] - time_dict[day_or_night]['start']).total_seconds() / 3600.0
    if not len(rel_df):
        return None
    # casting the value to float for writing the value to the api with the correct type
    return float(total_time_of_day - rel_df['total_time'].sum())


def get_number_events(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> Counter:
    """Get number of events during day/night

    Args:
        df (pd.DataFrame): events dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        Counter: returns counter that counts the number of each event
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    event_names = ['AcuteFall', 'ModerateFall', 'Long lying on the floor', 'Fall from bed']
    if not len(rel_df):
        return {event: 0 for event in event_names}
    return {event: int(sum(rel_df['type'] == event)) for event in event_names}


def get_sedentary(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get number of low activity (sedentary) during day/night

    Args:
        df (pd.DataFrame): events dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns the number of sedentary
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None
    # 1 is Low activity
    return int(sum(rel_df['activity'] == 1)) 


def get_gait_average(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> Tuple[float, float, float, float]:
    """Get gait average per day/night

    Args:
        df (pd.DataFrame): gait dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        tuple[float, float, float, float]: returns total distance, average speed, average number of sessions and average distance.
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None, None, None, None
    # casting the values to float for writing the values to the api with the correct type
    avg_sessions = float(rel_df['number_of_sessions'].mean())
    avg_time = float((rel_df['total_time'] / rel_df['number_of_sessions']).mean())
    avg_distance = float((rel_df['total_distance'] / rel_df['number_of_sessions']).mean())
    avg_speed = avg_distance / avg_time if avg_time else 0
    tot_distance = float(rel_df['total_distance'].sum())
    return tot_distance, avg_speed, avg_sessions, avg_distance
