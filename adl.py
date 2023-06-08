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
    df['start'] = pd.to_datetime(df['start'])
    df['stop'] = pd.to_datetime(df['stop'])
    df = df.sort_values('start')
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


def clustering_day_night(hours: pd.DatetimeIndex) -> tuple[int]:
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


def day_night_update_df(df, time_dict):
    df['day_or_night'] = ['Day'] * len(df)
    for day_or_night in time_dict:
        rel_time = (df['start'] >= time_dict[day_or_night]['start']) & (df['stop'] <= time_dict[day_or_night]['end'])
        df.loc[rel_time, 'day_or_night'] = day_or_night
    df['total_time'] = (df['stop'] - df['start']).astype('timedelta64[m]')
    return df


def get_day_night_times(df: pd.DataFrame) -> tuple[pd.DataFrame, dict[dict[str, pd.Timestamp]]]:
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
    return rel_df['total_time'].sum() / 60.0


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
        tuple[int, float]: _description_
    """
    rel_df = get_relevant_df(df, day_or_night)
    rel_df = rel_df[rel_df['location'] != 'Bed']
    return len(rel_df), rel_df['total_time'].sum() / 60.0


def get_location_distribution(df: pd.DataFrame, day_or_night: str) -> Counter:
    """Get location distribution during day/night

    Args:
        df (pd.DataFrame): location dataframe
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        Counter: counter for location distribution
    """
    rel_df = get_relevant_df(df, day_or_night)
    return Counter(rel_df['location'])


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
