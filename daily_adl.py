import pandas as pd
import numpy as np
import datetime
from collections import Counter, defaultdict

AWAKE = 0
SLEEP = 1

HOURS_PER_DAY = 24
SPLITS_PER_HOUR = 2
NUM_DAYS = 2

MAX_AWAKE = 3


def validate_df(df: pd.DataFrame) -> pd.DataFrame:
    """Validate dataframe, chage to timestamp format and sort.

    Args:
        df (pd.DataFrame): a dataframe

    Returns:
        pd.DataFrame: a dataframe that ready for further uses
    """
    for t in ['start', 'stop', 'time']:
        if t in df:
            df[t] = pd.to_datetime(df[t]).dt.tz_localize(None)
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
                                    day_or_night: str) -> tuple[int, defaultdict]:
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
        cond = (row['start'] < rel_location_df['start']) & \
            (row['stop'] > rel_location_df['stop']) & \
            (rel_location_df['location'] != 'Bed')
        if rel_location_df[cond]:
            number_out_of_bed += 1
            out_of_bed_duration += rel_location_df[cond]['total_time'].sum()
            for location in rel_location_df[cond]['location'].unique():
                counter[location] += sum(rel_location_df[cond]['location'] == location)
    return number_out_of_bed, out_of_bed_duration, counter


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


def clustering_day_night(hours: pd.DatetimeIndex) -> tuple[int, int]:
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


def day_night_update_df(df: pd.DataFrame, time_dict: dict) -> pd.DataFrame:
    """Update the dataframe by adding a column for day/night

    Args:
        df (pd.DataFrame): a dataframe
        time_dict (dict): a dictionary for the day night times

    Returns:
        pd.DataFrame: a dataframe that contains the day/night column
    """
    if 'day_or_night' in df:
        return df
    df['day_or_night'] = [None] * len(df)
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
    if len(dates) > 2:
        dates = dates[-2:]
    assert len(dates) <= 2, "There is more than 2 days"

    hours = making_hours_array(df, dates)

    night_start, night_end = clustering_day_night(hours)

    datetime_range = pd.date_range(start=dates[0], periods=len(hours) + 1, end=dates[1] + datetime.timedelta(days=1))

    day_delta = (0, 1) if df.iloc[-1]['stop'].time() > datetime.time(12, 0, 0) else (-1, 0)

    night_time = {
        'start': datetime_range[night_start],
        'end': datetime_range[night_end]
    }

    day_time = {
        'start': datetime_range[night_end] + datetime.timedelta(days=day_delta[0]),
        'end': datetime_range[night_start] + datetime.timedelta(days=day_delta[1])
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
    return rel_df['total_time'].sum()


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
    return (rel_df['restless'] * rel_df['total_time']).sum() / rel_df['total_time'].sum()


def get_out_of_bed_number(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> tuple[int, float]:
    """Get the number of out of bed and the duration

    Args:
        df (pd.DataFrame): location dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        tuple[int, float]: returns the number of out of bed times and the duration in hours
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    rel_df = rel_df[rel_df['location'] != 'Bed']
    return len(rel_df), rel_df['total_time'].sum()


def get_location_distribution(df: pd.DataFrame, time_dict: dict, day_or_night: str, bed_included: bool=False) -> dict[str, float]:
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
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None
    return rel_df['respiration'].mean()


def get_average_heartrate(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> float:
    """Get average heart-rate during day/night

    Args:
        df (pd.DataFrame): respiration dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        float: returns an average heart-rate
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None
    return rel_df['heart_rate'].mean()


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
    return total_time_of_day - rel_df['total_time'].sum()


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
    return Counter(rel_df['type'])


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
    return sum(rel_df['activity'] == 'Low')


def get_gait_average(df: pd.DataFrame, time_dict: dict, day_or_night: str) -> tuple[float, float, float]:
    """Get gait average per day/night

    Args:
        df (pd.DataFrame): gait dataframe
        time_dict (dict): a dictionary for the day night times
        day_or_night (str): can get the values 'Day' or 'Night'

    Returns:
        tuple[float, float, float]: returns average number of gait sessions, time and distance
    """
    rel_df = get_relevant_df(df, time_dict, day_or_night)
    if not len(rel_df):
        return None, None, None
    avg_sessions = rel_df['number_of_sessions'].mean()
    avg_time = rel_df['total_time'].mean()
    avg_distance = rel_df['total_distance'].mean()
    return avg_sessions, avg_time, avg_distance


if __name__ == '__main__':
    # sleep sessions
    sleep_sessions = {
    'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
    'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
    'restless': [0.8, 0.4, 0.9, 0.2]
    }

    sleep_df = pd.DataFrame(sleep_sessions)
    sleep_df, time_dict = get_day_night_times(sleep_df)
    print(f"Day Time: start: {time_dict['Day']['start']}, end: {time_dict['Day']['end']}")
    print(f"Night Time: start: {time_dict['Night']['start']}, end: {time_dict['Night']['end']}")

    night_sleep_duration = get_sleep_duration(sleep_df, time_dict, 'Night')
    day_sleep_duration = get_sleep_duration(sleep_df, time_dict, 'Day')
    night_restlessness = get_restlessness(sleep_df, time_dict, 'Night')
    day_restlessness = get_restlessness(sleep_df, time_dict, 'Day')
    print(f"The night sleep duration: {night_sleep_duration}, The night restlessness: {night_restlessness}")
    print(f"The day sleep duration: {day_sleep_duration}, The day restlessness: {day_restlessness}")

    # location
    location_data = {
        'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
        'location': ['Kitchen', 'Bed', 'Bedroom', 'Entry']
    }
    location_df = pd.DataFrame(location_data)
    night_out_of_bed_number, night_out_of_bed_duration = get_out_of_bed_number(location_df, time_dict, 'Night')
    night_location_distribution = get_location_distribution(location_df, time_dict, 'Night')
    day_location_distribution = get_location_distribution(location_df, time_dict, 'Day')
    print(f"Number of 'out of bed': {night_out_of_bed_number}, duration of 'out of bed': {night_out_of_bed_duration}")
    print(f"The distribution of locations of out of bed at night: {night_location_distribution}")
    print(f"The distribution of locations of out of bed at day: {day_location_distribution}")

    # respiration
    respiration_data = {
        'time': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'respiration': [30.2, 50.0, 45.1, 37.6]
    }
    respiration_df = pd.DataFrame(respiration_data)
    night_average_respiration = get_average_respiration(respiration_df, time_dict, 'Night')
    print(f"The average respiration at night: {night_average_respiration}")

    # visitors
    visitors_data = {
        'VisitorsIn': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'VisitorsOut': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00']
    }
    mapper_visitors = {'VisitorsIn': 'start', 'VisitorsOut': 'stop'}
    visitors_df = pd.DataFrame(visitors_data).rename(columns=mapper_visitors)
    total_alone_time = get_total_alone_time(visitors_df, time_dict, 'Day')
    print(f"The total alone time in day is: {total_alone_time}")

    # events
    events_data = {
        'time': ['2023-01-05 10:30:00', '2023-01-05 23:00:00', '2023-01-05 23:15:00', '2023-01-06 04:00:00', '2023-01-06 05:30:00', '2023-01-06 10:00:00', '2023-01-06 14:00:00'],
        'type': ['AcuteFall', 'AcuteFall', 'ModerateFall', 'LyingOnFloor', 'ModerateFall', 'LyingOnFloor', 'AcuteFall']
    }
    events_df = pd.DataFrame(events_data)
    night_events = get_number_events(events_df, time_dict, 'Night')
    day_events = get_number_events(events_df, time_dict, 'Day')
    print(f"The number of the events at night is: {dict(night_events)}")
    print(f"The number of the events at day is: {dict(day_events)}")

    # gait
    gait_data = {
        'time': pd.date_range(
            start=time_dict['Night']['start'],
            freq='1H',
            end=time_dict['Day']['end'] - datetime.timedelta(hours=1)
            ),
        'activity': [0, 0, 1, 1, 0, 0, 3, 1, 2, 0, 1, 1, 2, 2, 3, 0, 0, 1, 0, 1, 1, 2, 3, 2],
        'number_of_sessions': [3, 1, 0, 0, 0, 1, 0, 1, 2, 3, 2, 1, 0, 1, 2, 0, 1, 4, 5, 1, 2, 2, 1, 0],
        'total_time': [20, 10, 0, 0, 0, 15, 0, 10, 20, 30, 15, 12, 0, 10, 20, 0, 5, 40, 35, 10, 12, 14, 3, 0],
        'total_distance': [50, 20, 0, 0, 0, 25, 0, 5, 30, 40, 25, 17, 0, 12, 22, 0, 4, 80, 50, 20, 22, 30, 5, 0]
    }
    # 'activityLevel': [0, 0, 1, 1, 0, 0, 3, 1, 2, 0, 1, 1, 2, 2, 3, 0, 0, 1, 0, 1, 1, 2, 3, 2],
    # 'numberOfWalkingSessions': [3, 1, 0, 0, 0, 1, 0, 1, 2, 3, 2, 1, 0, 1, 2, 0, 1, 4, 5, 1, 2, 2, 1, 0],
    # 'totalWalkDuration': [20, 10, 0, 0, 0, 15, 0, 10, 20, 30, 15, 12, 0, 10, 20, 0, 5, 40, 35, 10, 12, 14, 3, 0],
    # 'totalWalkDistance': [50, 20, 0, 0, 0, 25, 0, 5, 30, 40, 25, 17, 0, 12, 22, 0, 4, 80, 50, 20, 22, 30, 5, 0]
    gait_df = pd.DataFrame(gait_data)
    night_sedentary = get_sedentary(gait_df, time_dict, 'Night')
    day_sedentary = get_sedentary(gait_df, time_dict, 'Day')
    print(f"The number of the sedentary at night is: {night_sedentary}")
    print(f"The number of the sedentary at day is: {day_sedentary}")
    average_gait_sessions, average_gait_time, average_gait_distance = get_gait_average(gait_df, time_dict, 'Day')
    print(f"The daily average number of gait sessions is: {average_gait_sessions}")
    print(f"The daily average time of gait sessions is: {average_gait_time}")
    print(f"The daily average distance of gait sessions is: {average_gait_distance}")
    
    # sleep location
    count_out_of_bed_loaction_sleep(sleep_df, location_df, 'Night')