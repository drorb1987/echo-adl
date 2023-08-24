import pandas as pd
import numpy as np
import os
import scipy.stats as stats
import json
import daily_adl
from cloud_api import warp_location_df, warp_gait_df, warp_respiration_df, warp_sleep_df

CSV_PATH = r'.\data_csvs'


def read_sleep_logs():
    path = os.path.join(CSV_PATH, 'SleepLogs.csv')
    df = pd.read_csv(path, header=None, names=['sessionStartTime', 'sessionStopTime',
                                               'sessionRestless'])
    df.index = pd.to_datetime(df['sessionStopTime'])
    return df


def read_respiration_logs():
    path = os.path.join(CSV_PATH, 'RespirationLogs.csv')
    df = pd.read_csv(path, header=None, names=['respirationTime', 'respirationRate',
                                               'heartRate'])
    df.index = pd.to_datetime(df['respirationTime'])
    return df


def read_location_logs():
    path = os.path.join(CSV_PATH, 'LocationLogs.csv')
    df = pd.read_csv(path, header=None, names=['locationStartTime', 'locationStopTime',
                                               'locationName'],
                     index_col=False)
    df.index = pd.to_datetime(df['locationStopTime'])
    return df


def read_gait_logs():
    path = os.path.join(CSV_PATH, 'GaitLogs.csv')
    df = pd.read_csv(path, index_col='Day')
    return df


def get_extended_report(start_date, end_date):
    df_sleep = read_sleep_logs()
    df_respiration = read_respiration_logs()
    df_location = read_location_logs()
    df_gait = read_gait_logs()
    days = pd.date_range(start_date, end_date, freq='d')
    output_json = []
    for day in days:
        output_dict = {'data': {'locations': {'objects': None}, 'sleepMonitoring': None,
                       'respirations': None, 'gaitAnalysis': None}, 'timeStamp': day.strftime('%Y-%m-%dT%H:%M%S.000Z')}
        day_string = day.date().strftime('%Y-%m-%d')
        try:
            output_dict['data']['locations']['objects'] = \
                df_location.loc[day_string].to_dict('records')
        # date doesn't exist
        except KeyError:
            output_dict['data']['locations']['objects'] = []

        try:
            filtered_sleep_df = df_sleep.loc[day_string]
            filtered_sleep_df['sessionIndex'] = np.arange(len(filtered_sleep_df))
            output_dict['data']['sleepMonitoring'] = filtered_sleep_df.to_dict('records')
        except KeyError:
            output_dict['data']['sleepMonitoring'] = []

        # empty values in the JSON returned from the db are null - these can be parsed by json library
        # as None - to any na values in the original csv are converted to None
        # there are ways through pandas but it changes the dtype to object
        try:
            output_dict['data']['respirations'] = df_respiration.loc[day_string].to_dict('records')
            for val in output_dict['data']['respirations']:
                for key, item in val.items():
                    if pd.isnull(item):
                        val[key] = None

        except KeyError:
            output_dict['data']['respirations'] = []
        # should not be empty fields here I don't think
        output_dict['data']['gaitAnalysis'] = df_gait.loc[day_string,
                                                          ['numberOfWalkingSessions', 'totalWalkDuration',
                                                           'totalWalkDistance', 'activityLevel']].to_dict('records')

        output_json.append(output_dict)
    return output_json


def sim_gait(start_date, end_date):
    activity_level = stats.multinomial(1, [0.4, 0.4, 0.19, 0.01]).rvs(264).argmax(1)
    avg_walk_sessions = stats.weibull_min(2, loc=3, scale=1).rvs(264)
    avg_walk_duration = stats.weibull_min(3).rvs(264)
    avg_walk_distance = stats.norm(7, 1).rvs(264)
    times = pd.date_range(start_date, end_date, freq='h')[:-1]
    df = pd.DataFrame(data={'Day': [t.date().strftime('%Y-%m-%d') for t in times], 'hour': [t.hour for t in times],
                            'numberOfWalkingSessions': avg_walk_sessions, 'totalWalkDuration': avg_walk_duration,
                            'totalWalkDistance': avg_walk_distance, 'activityLevel': activity_level})
    df.to_csv('GaitLogs.csv')


if __name__ == "__main__":
    # input_json = get_extended_report('2023-06-12', '2023-06-21')
    # with open(os.path.join(CSV_PATH, 'sample_data.json'), 'w') as f:
    #     json.dump(input_json, f)

    # sim_gait('2023-06-11', '2023-06-22')

    # read json
    with open(os.path.join(CSV_PATH, 'sample_data.json')) as f:
        response = json.load(f)
    sleep_res = np.zeros((len(response) - 1, 10), dtype=object)
    location_res = np.zeros((len(response) - 1, 5))
    activity_res = np.zeros((len(response) - 1, 5))
    prev_res = {}
    for i, res in enumerate(response):
        if not prev_res:
            prev_res = res.copy()
            continue
        curr_date = pd.to_datetime(res['timeStamp']).date()
        prev_sleep_df = warp_sleep_df(prev_res)
        sleep_df = warp_sleep_df(res)
        sleep_df = pd.concat([prev_sleep_df, sleep_df], axis=0, ignore_index=True)

        sleep_df, time_dict = daily_adl.get_day_night_times(sleep_df)
        night_sleep_duration = daily_adl.get_sleep_duration(sleep_df, time_dict, 'Night')
        day_sleep_duration = daily_adl.get_sleep_duration(sleep_df, time_dict, 'Day')
        night_restlessness = daily_adl.get_restlessness(sleep_df, time_dict, 'Night')
        day_restlessness = daily_adl.get_restlessness(sleep_df, time_dict, 'Day')

        # location
        prev_location_df = warp_location_df(prev_res)
        location_df = warp_location_df(res)
        location_df = pd.concat([prev_location_df, location_df], axis=0, ignore_index=True)

        location_df = daily_adl.create_consecutive_df(location_df)
        number_out_of_bed, night_out_of_bed_duration, location_counter = daily_adl.count_out_of_bed_loaction_sleep(
            sleep_df, location_df, time_dict, 'Night')
        daily_location_distribution = daily_adl.get_location_distribution(location_df, time_dict, 'Day',
                                                                          bed_included=True)

        # respiration
        prev_respiration_df = warp_respiration_df(prev_res)
        respiration_df = warp_respiration_df(res)
        respiration_df = pd.concat([prev_respiration_df, respiration_df], axis=0, ignore_index=True)
        average_respiration = daily_adl.get_average_respiration(respiration_df, time_dict, 'Night')
        average_heartrate = daily_adl.get_average_heartrate(respiration_df, time_dict, 'Night')

        # gait
        gait_df = warp_gait_df(res, time_dict)
        daily_sedantery = daily_adl.get_sedentary(gait_df, time_dict, 'Day')
        total_gait_distance, average_gait_speed, average_gait_sessions, average_gait_distance = daily_adl.get_gait_average(
            gait_df, time_dict, 'Day')

        prev_res = res.copy()
        go_to_sleep_return = str(time_dict["Night"]["start"]) if time_dict is not None else None
        wake_up_return = str(time_dict["Night"]["end"]) if time_dict is not None else None
        location_counter_return = location_counter if location_counter is not None else None
        sleep_res[i - 1, :] = [night_sleep_duration, night_restlessness, go_to_sleep_return,
                               wake_up_return, number_out_of_bed, night_out_of_bed_duration,
                               day_sleep_duration, location_counter_return, average_respiration, average_heartrate]
        try:
            location_res[i - 1, :] = [loc for loc in daily_location_distribution.values()]
        except ValueError:
            location_res[i - 1, :] = np.zeros((1, 5))
        activity_res[i - 1, :] = [daily_sedantery, total_gait_distance, average_gait_speed,
                                  average_gait_sessions, average_gait_distance]

    df_sleep_res = pd.DataFrame(sleep_res, columns=['Night Sleep Duration', 'Night Restlessness', 'Go to sleep time',
                                                    'Wake up time', 'number out of bed', 'out of bed duration',
                                                    'day sleep duration', 'out of bed location distribution',
                                                    'average respiration', 'average heartrate'])
    # in hours
    try:
        df_location_res = pd.DataFrame(location_res, columns=list(daily_location_distribution.keys()))
    except ValueError:
        df_location_res = None
        print('No locations available')
    df_activity_res = pd.DataFrame(activity_res, columns=['daily sedentary', 'total_gait_distance',
                                                          'average_gait_speed', 'average_gait_sessions',
                                                          'average_gait_distance'])
    print(df_activity_res)
    print(df_location_res)
    print(df_sleep_res)
    # df_sleep_res.to_csv('sleep_res.csv')