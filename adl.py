import pandas as pd
import numpy as np
import datetime

AWAKE = 0
SLEEP = 1

HOURS_PER_DAY = 24
SPLITS_PER_HOUR = 2
NUM_DAYS = 2

MAX_AWAKE = 3


def validate_df(df: pd.DataFrame) -> None:
    df['start'] = pd.to_datetime(df['start'])
    df['stop'] = pd.to_datetime(df['stop'])
    df = df.sort_values('start')


def making_hours_array(df: pd.DataFrame, dates: pd.DatetimeIndex) -> np.ndarray:
    hours = np.full(NUM_DAYS * SPLITS_PER_HOUR * HOURS_PER_DAY, AWAKE)
    for _, row in df.iterrows():
        # start time index
        is_curr_date = pd.Timestamp(row['start'].date()) == dates[-1]
        h = row['start'].time().hour
        m = row['start'].time().minute
        start_index = is_curr_date * HOURS_PER_DAY * SPLITS_PER_HOUR + h * SPLITS_PER_HOUR + np.floor((m / 60) * SPLITS_PER_HOUR)
        start_index = max(start_index, 0)
        # stop time index
        is_curr_date = pd.Timestamp(row['stop'].date()) == dates[-1]
        h = row['stop'].time().hour
        m = row['stop'].time().minute
        stop_index = is_curr_date * HOURS_PER_DAY * SPLITS_PER_HOUR + h * SPLITS_PER_HOUR + np.ceil((m / 60) * SPLITS_PER_HOUR)
        stop_index = min(len(hours) - 1, stop_index) + 1
        # assigning the sleep session time to the hours array
        hours[start_index: stop_index] = SLEEP
    return hours


def clustering_day_night(hours: pd.DatetimeIndex) -> tuple[int]:
    # clustering for day and night
    mid = len(hours) // 2
    night_start = mid - 1
    night_end = mid + 1
    start_awake_indices = np.where(hours[:mid] == AWAKE)
    start_awake_indices.reverse()
    end_awake_indices = np.where(hours[mid:] == AWAKE)

    for night_start in start_awake_indices:
        if np.all(hours[max(0, night_start - MAX_AWAKE): night_start+1] == AWAKE):
            break
    for night_end in end_awake_indices:
        if np.all(hours[night_end: min(night_end+MAX_AWAKE+1, len(hours) - 1)] == AWAKE):
            break
    return night_start, night_end


def get_day_night_times(df: pd.DataFrame) -> tuple[dict[str, pd.Timestamp]]:

    validate_df(df)

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

    return day_time, night_time


def get_rel_df(df: pd.DataFrame, time: pd.Timestamp) -> pd.DataFrame:
    rel_time = time['start'] < df['start'] < time['end']
    df_rel_time = df.loc[rel_time]
    df_rel_time['total_time'] = (df_rel_time['stop'] - df_rel_time['start']).astype('timedelta64[m]')
    return df_rel_time


def get_sleep_duration(df: pd.DataFrame, time: pd.Timestamp) -> float:
    df_rel_time = get_rel_df(df, time)
    return df_rel_time['total_time'].sum() / 60.0


def get_restlessness(df: pd.DataFrame, night_time: pd.Timestamp) -> float:
    df_rel_time = get_rel_df(df, night_time)
    return (df_rel_time['data'] * df_rel_time['total_time']).sum() / df_rel_time['total_time'].sum()


def get_wake_duration(df: pd.DataFrame, time: pd.Timestamp) -> float:
    df_rel_time = get_rel_df(df, time)
    total_time = (time['end'] - time['start']).astype('timedelta64[m]')
    return (total_time - df_rel_time['total_time'].sum()) / 60.0


def get_out_of_bed_number(df: pd.DataFrame, time: pd.Timestamp) -> tuple[int, float]:
    df_rel_time = get_rel_df(df, time)
    df_rel_time = df_rel_time.loc[df_rel_time['location'] != 'Bed']
    return len(df_rel_time), df_rel_time['total_time'].sum() / 60.0
