import pandas as pd
import numpy as np
import datetime

AWAKE = 0
SLEEP = 1

HOURS_PER_DAY = 24
SPLITS_PER_HOUR = 2
NUM_DAYS = 2

MAX_AWAKE = 3

class ADL_model:

    def __init__(self) -> None:
        self.sleep_df = None
        self.location_df = None
        self.respiration_df = None
        self.visitors_df = None
        self.dates = None
        self.hours = np.full(NUM_DAYS * SPLITS_PER_HOUR * HOURS_PER_DAY, AWAKE)
        self.time = None

    def set_sleep_df(self, sleep_data: dict) -> None:
        self.sleep_df = pd.DataFrame(sleep_data)
        self.validate_df(self.sleep_df)
        self.sleep_df = self.sleep_df.sort_values('start', ignore_index=True)
        self.add_total_time(self.sleep_df)
        self.dates = pd.to_datetime(pd.concat([self.sleep_df['start'], self.sleep_df['stop']]).dt.date.unique()).sort_values()
        self.set_hours()
        night_start, night_end = self.clustering_day_night()
        self.set_time(night_start, night_end)
        self.day_night_update_df(self.sleep_df)
    
    def set_location_df(self, location_data: dict) -> None:
        assert self.time is not None, "You must first set the sleep dataframe."
        self.location_df = pd.DataFrame(location_data)
        self.validate_df(self.location_df)
        self.location_df = self.location_df.sort_values('start', ignore_index=True)
        self.add_total_time(self.location_df)
        self.day_night_update_df(self.location_df)
    
    def set_respiration_df(self, respiration_data: dict) -> None:
        assert self.time is not None, "You must first set the sleep dataframe."
        self.respiration_df = pd.DataFrame(respiration_data)
        self.validate_df(self.respiration_df)
        self.respiration_df = self.respiration_df.sort_values('time', ignore_index=True)
        self.day_night_update_df(self.respiration_df)

    def validate_df(self, df: pd.DataFrame) -> None:
        for t in ['start', 'stop', 'time']:
            if t in df:
                df[t] = pd.to_datetime(df[t])
    
    def add_total_time(self, df: pd.DataFrame) -> None:
        df['total_time'] = (df['stop'] - df['start']).astype('timedelta64[m]') / 60.0

    def set_hours(self) -> None:
        for _, row in self.sleep_df.iterrows():
            # start time index
            is_curr_date = pd.Timestamp(row['start'].date()) == self.dates[-1]
            h = row['start'].time().hour
            m = row['start'].time().minute
            start_index = is_curr_date * HOURS_PER_DAY * SPLITS_PER_HOUR + h * SPLITS_PER_HOUR + np.floor((m / 60) * SPLITS_PER_HOUR)
            start_index = int(max(start_index, 0))
            # stop time index
            is_curr_date = pd.Timestamp(row['stop'].date()) == self.dates[-1]
            h = row['stop'].time().hour
            m = row['stop'].time().minute
            stop_index = is_curr_date * HOURS_PER_DAY * SPLITS_PER_HOUR + h * SPLITS_PER_HOUR + np.ceil((m / 60) * SPLITS_PER_HOUR)
            stop_index = int(min(len(self.hours) - 1, stop_index) + 1)
            # assigning the sleep session time to the hours array
            self.hours[start_index: stop_index] = SLEEP

    def clustering_day_night(self) -> tuple[int, int]:
        # clustering for day and night
        mid = len(self.hours) // 2
        night_start = mid - 1
        night_end = mid + 1
        start_awake_indices = np.where(self.hours[:mid] == AWAKE)
        end_awake_indices = np.where(self.hours[mid:] == AWAKE)

        for night_start in start_awake_indices[0][::-1]:
            if np.all(self.hours[max(0, night_start - MAX_AWAKE): night_start+1] == AWAKE):
                break
        for night_end in end_awake_indices[0] + mid:
            if np.all(self.hours[night_end: min(night_end+MAX_AWAKE+1, len(self.hours) - 1)] == AWAKE):
                break
        return night_start+1, night_end
    
    def set_time(self, night_start: int, night_end: int) -> None:
        datetime_range = pd.date_range(
            start=self.dates[0], 
            periods=len(self.hours) + 1, 
            end=self.dates[1] + datetime.timedelta(days=1)
            )

        night_time = {
            'start': datetime_range[night_start],
            'end': datetime_range[night_end]
        }

        day_time = {
            'start': datetime_range[night_end],
            'end': datetime_range[night_start] + datetime.timedelta(days=1)
        }

        self.time  = {
            'Day': day_time,
            'Night': night_time
        }

    def day_night_update_df(self, df: pd.DataFrame) -> None:
        df['day_or_night'] = ['Day'] * len(df)
        start = 'start' if 'start' in df else 'time'
        stop = 'stop' if 'stop' in df else 'time'
        for day_or_night in self.time:
            rel_time = (df[start] >= self.time[day_or_night]['start']) & (df[stop] <= self.time[day_or_night]['end'])
            df.loc[rel_time, 'day_or_night'] = day_or_night
    
    def get_relevant_df(self, df: pd.DataFrame, day_or_night: str) -> pd.DataFrame:
        assert day_or_night in ['Day', 'Night'], "day_or_night can get values 'Day' or 'Night'"
        return df[df['day_or_night'] == day_or_night]



if __name__ == '__main__':
    adl_model = ADL_model()
    # sleep_sessions = {
    # 'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
    # 'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
    # 'restless': [0.8, 0.4, 0.9, 0.2]
    # }
    sleep_sessions = {
    'start': ['2023-01-06 18:20:00', '2023-01-05 10:30:00', '1/5/2023 15:45:00', '2023-01-05 22:15:00'],
    'stop': ['2023-01-06 19:00:00', '2023-01-05 11:00:00', '2023-01-05 18:00:00', '1/6/2023 07:15:00'],
    'restless': [0.8, 0.4, 0.9, 0.2]
    }
    adl_model.set_sleep_df(sleep_sessions)
    print(adl_model.sleep_df)