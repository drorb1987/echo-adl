import pandas as pd
import numpy as np
import datetime
from collections import Counter

AWAKE = 0
SLEEP = 1

HOURS_PER_DAY = 24
SPLITS_PER_HOUR = 2
NUM_DAYS = 2

MAX_AWAKE = 3



def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    for t in ['start', 'stop', 'time']:
        if t in df:
            df[t] = pd.to_datetime(df[t])
            if t is not 'stop':
                df = df.sort_values(t)
    return df


def making_hours_array(df: pd.DataFrame, dates: pd.DatetimeIndex) -> np.ndarray:
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


def clustering_day_night(hours: pd.DatetimeIndex) -> tuple[int, int]:
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


def day_night_update_df(df: pd.DataFrame, time_dict: dict[str, dict[str, pd.Timestamp]]) -> pd.DataFrame:
    df['day_or_night'] = ['Day'] * len(df)
    start = 'start' if 'start' in df else 'time'
    stop = 'stop' if 'stop' in df else 'time'
    for day_or_night in time_dict:
        rel_time = (df[start] >= time_dict[day_or_night]['start']) & (df[stop] <= time_dict[day_or_night]['end'])
        df.loc[rel_time, 'day_or_night'] = day_or_night
    if 'start' in df:
        df['total_time'] = (df['stop'] - df['start']).astype('timedelta64[m]') / 60.0
    return df


def get_day_night_times(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, dict[str, pd.Timestamp]]]:
    """Get the day and night times from the sleep sessions data frame

    Args:
        df (pd.DataFrame): The sleep sessions data frame 

    Returns:
        tuple[dict[str, pd.Timestamp]]: day_time, night_time each have 'start' and 'end' timestamp
    """

    df = validate_df(df)

    dates = pd.to_datetime(pd.concat([df['start'], df['stop']]).dt.date.unique()).sort_values()
    assert len(dates) <= 2, "There is more than 2 days"

    hours = making_hours_array(df, dates)

    night_start, night_end = clustering_day_night(hours)

    datetime_range = pd.date_range(start=dates[0], periods=len(hours) + 1, end=dates[1] + datetime.timedelta(days=1))

    night_time = {
        'start': datetime_range[night_start],
        'end': datetime_range[night_end]
    }

    day_time = {
        'start': datetime_range[night_end],
        'end': datetime_range[night_start] + datetime.timedelta(days=1)
    }

    time_dict  = {
        'Day': day_time,
        'Night': night_time
    }

    df = day_night_update_df(df, time_dict)

    return df, time_dict


def get_relevant_df(df: pd.DataFrame, day_or_night: str) -> pd.DataFrame:
    assert day_or_night in ['Day', 'Night'], "day_or_night can get values 'Day' or 'Night'"
    return df[df['day_or_night'] == day_or_night]


def get_sleep_duration(df: pd.DataFrame, day_or_night: str) -> float:
    """Get the sleep duration on day/night

    Args:
        df (pd.DataFrame): sleep sessoions data frame
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: The sleep duration in hours
    """
    rel_df = get_relevant_df(df, day_or_night)
    return rel_df['total_time'].sum()


def get_restlessness(df: pd.DataFrame, day_or_night: str) -> float:
    """Get restlessness during sleep per day/night

    Args:
        df (pd.DataFrame): sleep sessions data frame
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: average restlessness
    """
    rel_df = get_relevant_df(df, day_or_night)
    return (rel_df['restless'] * rel_df['total_time']).sum() / rel_df['total_time'].sum()


def get_out_of_bed_number(df: pd.DataFrame, day_or_night: str) -> tuple[int, float]:
    """Get the number of out of bed and the duration

    Args:
        df (pd.DataFrame): location dataframe
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        tuple[int, float]: returns the number of out of bed times and the duration in hours
    """
    rel_df = get_relevant_df(df, day_or_night)
    rel_df = rel_df[rel_df['location'] != 'Bed']
    return len(rel_df), rel_df['total_time'].sum()


def get_location_distribution(df: pd.DataFrame, day_or_night: str) -> dict[str, float]:
    """Get location distribution during day/night

    Args:
        df (pd.DataFrame): location dataframe
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        dict[str, float]: returns a dictionary for the locations distribution
    """
    rel_df = get_relevant_df(df, day_or_night)
    location_dist = rel_df.groupby('location')['total_time'].sum().to_dict()
    if 'Bed' in location_dist:
        location_dist.pop('Bed')
    return location_dist


def get_average_respiration(df: pd.DataFrame, day_or_night: str) -> float:
    """Get average respiration during day/night

    Args:
        df (pd.DataFrame): respiration dataframe
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns an average respiration
    """
    rel_df = get_relevant_df(df, day_or_night)
    return rel_df['respiration'].apply('average')


def get_total_alone_time(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get total alone time during day/night

    Args:
        df (pd.DataFrame): visitors dataframe
        time_dict (dict): a dictionary for the start time and and time for day/night
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns total alone time
    """
    rel_df = get_relevant_df(df, day_or_night)
    total_time_of_day = (time_dict[day_or_night]['end'] - time_dict[day_or_night]['start']).total_seconds() / 3600.0
    return total_time_of_day - rel_df['total_time'].sum()


def get_number_events(df: pd.DataFrame, day_or_night: str) -> Counter:
    """Get number of events during day/night

    Args:
        df (pd.DataFrame): events dataframe
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        Counter: returns counter that counts the number of each event
    """
    rel_df = get_relevant_df(df, day_or_night)
    return Counter(rel_df['event'])


if __name__ == '__main__':
    sleep_sessions = {
    'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
    'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
    'restless': [0.8, 0.4, 0.9, 0.2]
    }

    sleep_df = pd.DataFrame(sleep_sessions)
    sleep_df, time_dict = get_day_night_times(sleep_df)
    night_sleep_duration = get_sleep_duration(sleep_df, 'Night')
    day_sleep_duration = get_sleep_duration(sleep_df, 'Day')
    night_restlessness = get_restlessness(sleep_df, 'Night')
    day_restlessness = get_restlessness(sleep_df, 'Day')

    location_data = {
        'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
        'location': ['Kitchen', 'Bed', 'Bedroom', 'Entry']
    }
    location_df = pd.DataFrame(location_data)
    location_df = validate_df(location_df)
    location_df = day_night_update_df(location_df, time_dict)
    night_out_of_bed_number, night_out_of_bed_duration = get_out_of_bed_number(location_df, 'Night')
    night_location_distribution = get_location_distribution(location_df, 'Night')
    day_location_distribution = get_location_distribution(location_df, 'Day')

    respiration_data = {
        'time': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'respiration': [30.2, 50.0, 45.1, 37.6]
    }
    respiration_df = pd.DataFrame(respiration_data)
    respiration_df = validate_df(respiration_df)
    respiration_df = day_night_update_df(respiration_df, time_dict)
    night_average_respiration = get_average_respiration(respiration_df, 'Night')

    visitors_data = {
        'VisitorsIn': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'VisitorsOut': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00']
    }
    mapper_visitors = {'VisitorsIn': 'start', 'VisitorsOut': 'stop'}
    visitors_df = pd.DataFrame(visitors_data).rename(columns=mapper_visitors)
    visitors_df = validate_df(visitors_df)
    visitors_df = day_night_update_df(visitors_df, time_dict)
    total_alone_time = get_total_alone_time(visitors_df, time_dict, 'Day')

    events_data = {
        'time': ['2023-01-05 10:30:00', '2023-01-05 23:00:00', '2023-01-05 23:15:00', '2023-01-06 04:00:00', '2023-01-06 05:30:00', '2023-01-06 10:00:00', '2023-01-06 14:00:00'],
        'event': ['AcuteFall', 'AcuteFall', 'ModerateFall', 'LyingOnFloor', 'ModerateFall', 'LyingOnFloor', 'AcuteFall']
    }
    events_df = pd.DataFrame(events_data)
    events_df = validate_df(events_df)
    events_df = day_night_update_df(events_df, time_dict)
    night_events = get_number_events(events_df, 'Night')
    day_events = get_number_events(events_df, 'Day')
