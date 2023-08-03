#!/usr/bin/env python

import requests
import argparse
import pandas as pd
import datetime
import daily_adl, monthly_adl

DeviceIdFile = "./Certificates/Certificate/DeviceId.key"
publicKeyFile = "./Certificates/Certificate/publicKey.key"

url_extended_report     = "https://backend-dev.echocare-ncs.com/api/device/extendedReport"
url_get_extended_report = "https://backend-dev.echocare-ncs.com/api/device/getExtendedReport"

url_get_emergenies_report = "https://backend-dev.echocare-ncs.com/api/device/getEmergencies"

url_extended_analyze_report = "https://backend-dev.echocare-ncs.com/api/device/setAnalyzedReport"
url_get_extended_analyze_report = "https://backend-dev.echocare-ncs.com/api/device/getAnalyzedReport"

url_extended_statistics_report = "https://backend-dev.echocare-ncs.com/api/device/setStatisticsReport"
url_get_extended_statistics_report = "https://backend-dev.echocare-ncs.com/api/device/getStatisticsReport"

api_key = "wH2JyNCYzeoxmdJdHlizvzVneyDB92B4yXOyPtTH4ulP07uWIPoUDiRY32i1ZKVwodGw6Ecgu1zEYmC0HElntLoPLp1J58bGwXcJ6VJgfYszi8BBOTHa6DBfg6qb2Dwi"


def enumOpcodeReadPubKeyConfig() -> str:
    with open(publicKeyFile, 'r') as file:
        publicKey = file.read().replace('\n', '')

    print(publicKey)
    return publicKey


def enumOpcodeReadIdConfig() -> str:
    with open(DeviceIdFile, 'r') as file:
        DeviceId = file.read().replace('\n', '')

    print(DeviceId)
    return DeviceId


def warp_sleep_df(res: dict) -> pd.DataFrame:
    """Warping the response to handle the sleep sessions and convert it to data-frame

    Args:
        res (dict): request response from the api

    Returns:
        pd.DataFrame: A data-frame to handle the sleep sessions
    """
    sleep_mapper = {
        'sessionStartTime': 'start',
        'sessionStopTime': 'stop',
        'sessionRestless': 'restless'
    }
    sleep_columns = ['start', 'stop', 'restless']
    sleep_df = pd.DataFrame(res['data']['sleepMonitoring']).rename(columns=sleep_mapper)
    return sleep_df[sleep_columns]
    

def warp_location_df(res: dict) -> pd.DataFrame:
    """Warping the response to handle the locations and convert it to data-frame

    Args:
        res (dict): request response from the api

    Returns:
        pd.DataFrame: A data-frame to handle the locations
    """
    location_mapper = {
        'locationStartTime': 'start',
        'locationStopTime': 'stop',
        'locationName': 'location'
    }
    location_columns = ['start', 'stop', 'location']
    location_df = pd.DataFrame(res['data']['locations']['objects']).rename(columns=location_mapper)
    return location_df[location_columns]


def warp_respiration_df(res: dict) -> pd.DataFrame:
    """Warping the response to handle the respiration and convert it to data-frame

    Args:
        res (dict): request response from the api

    Returns:
        pd.DataFrame: A data-frame to handle the respiration
    """
    respiration_mapper = {
        'respirationTime': 'time',
        'respirationRate': 'respiration',
        'heartRate': 'heart_rate'
    }
    respiration_columns = ['time', 'respiration', 'heart_rate']
    respiration_df = pd.DataFrame(res['data']['respirations']).rename(columns=respiration_mapper)
    if not len(respiration_df):
        return respiration_df
    return respiration_df[respiration_columns]


def warp_gait_df(res: dict, time_dict: dict) -> pd.DataFrame:
    """Warping the response to handle the gait and convert it to data-frame

    Args:
        res (dict): request response from the api

    Returns:
        pd.DataFrame: A data-frame to handle the gait
    """
    start_time = pd.to_datetime(time_dict['Day']['start'].date())
    times = pd.date_range(
        start=start_time,
        freq='1H',
        end=start_time+datetime.timedelta(hours=23)
    )
    gait_mapper = {
        'numberOfWalkingSessions': 'number_of_sessions',
        'totalWalkDistance': 'total_distance',
        'totalWalkDuration': 'total_time',
        'activityLevel': 'activity'
    }
    gait_columns = ['number_of_sessions', 'total_distance', 'total_time', 'activity', 'time']
    gait_df = pd.DataFrame(res['data']['gaitAnalysis']).rename(columns=gait_mapper)
    assert len(gait_df) == 24, "Need to be data for the last 24 hours"
    gait_df['time'] = times
    return gait_df[gait_columns]


def warp_alerts_df(response: dict) -> pd.DataFrame:
    """Warping the response to handle the alerts and convert it to data-frame

    Args:
        res (dict): request response from the api

    Returns:
        pd.DataFrame: A data-frame to handle the alerts
    """
    data = list(map(lambda x: x['data'], response))
    df = pd.DataFrame(data)
    if not len(df):
        return pd.DataFrame(columns=['type', 'location', 'description', 'date', 'time', 'date_time'])
    df['date_time'] = pd.to_datetime(df['date'] + ' ' + df['time'])
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date_time', ignore_index=True)
    return df


def warp_visitors_df(alerts_df: pd.DataFrame) -> pd.DataFrame:
    """Warping the response to handle the visitors and convert it to data-frame

    Args:
        res (dict): request response from the api

    Returns:
        pd.DataFrame: A data-frame to handle the visitors
    """
    visitors_indices = (alerts_df['type'] == 'VisitorsIn') | (alerts_df['type'] == 'VisitorsOut')
    df = alerts_df[visitors_indices]
    start_time = []
    stop_time = []
    for i in range(len(df)-1):
        if df.loc[i, 'type'] == 'VisitorsIn' and df.loc[i+1, 'type'] == 'VisitorsOut':
            start_time.append(df.loc[i, 'date_time'])
            stop_time.append(df.loc[i+1, 'date_time'])
    return pd.DataFrame({'start': start_time, 'stop': stop_time})


def daily_analyse_api(device_id: str, from_date: str, to_date: str) -> None:
    """API for daily analysis ADL

    Args:
        device_id (str): device id
        from_date (str): start date
        to_date (str): end date
    """
    querystring = {
        "deviceId": device_id,
        "from": from_date,
        "to": to_date
    }
    headers = {
        'x-api-key': api_key
    }
    response = requests.request(
        "GET",
        url_get_extended_report,
        headers=headers,
        params=querystring
    )

    response_alert = requests.request(
        "GET",
        url_get_emergenies_report,
        headers=headers,
        params=querystring
    )

    response_alert_dict = response_alert.json()

    alerts_df = warp_alerts_df(response_alert_dict)
    visitors_df = warp_visitors_df(alerts_df)

    public_key = enumOpcodeReadPubKeyConfig()
    
    analyse_body = []

    prev_res = {}

    for res in response.json():
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
        number_out_of_bed, night_out_of_bed_duration, location_counter = daily_adl.count_out_of_bed_loaction_sleep(sleep_df, location_df, time_dict, 'Night')
        daily_location_distribution = daily_adl.get_location_distribution(location_df, time_dict, 'Day', bed_included=True)

        # respiration
        prev_respiration_df = warp_respiration_df(prev_res)
        respiration_df = warp_respiration_df(res)
        respiration_df = pd.concat([prev_respiration_df, respiration_df], axis=0, ignore_index=True)
        average_respiration = daily_adl.get_average_respiration(respiration_df, time_dict, 'Night')
        average_heartrate = daily_adl.get_average_heartrate(respiration_df, time_dict, 'Night')

        # gait
        gait_df = warp_gait_df(res, time_dict)
        daily_sedantery = daily_adl.get_sedentary(gait_df, time_dict, 'Day')
        total_gait_distance, average_gait_speed, average_gait_sessions, average_gait_distance = daily_adl.get_gait_average(gait_df, time_dict, 'Day')

        # alerts
        events_counter = daily_adl.get_number_events(alerts_df, time_dict, 'Day')
        alone_time = daily_adl.get_total_alone_time(visitors_df, time_dict, 'Day')

        prev_res = res.copy()

        analyse_params = {
            "deviceId": device_id,
            "publicKey": public_key,
            "data": {
                "sleepDuration": night_sleep_duration,
                "restlessness": night_restlessness,
                "goToSleepTime": str(time_dict["Night"]["start"]),
                "wakUpTime": str(time_dict["Night"]["end"]),
                "numberOfOutOfBedDuringNight": number_out_of_bed,
                "durationOfOutOfBed": night_out_of_bed_duration,
                "sleepDurationDuringDay": day_sleep_duration,

                "locationDistributionOfOutOfBedDuringNight": dict(location_counter),

                "averageNightlyRR": average_respiration,
                "locationDistributionDuringDay": daily_location_distribution,
                "sedentaryDurationDuringDay": daily_sedantery,
                "aloneTime": alone_time,
                "numberOfAcuteFalls": events_counter['acuteFall'],
                "numberOfModerateFalls": events_counter['moderateFall'],
                "numberOfLyingOnFloor": events_counter['lyingOnFloor'],
                "gaitStatisticsDuringDay": [total_gait_distance, average_gait_speed, average_gait_sessions, average_gait_distance]

            }
        }

        analyse_body.append(analyse_params)

        # Write the analyse params to the DB using the API
        headers_analyse = headers.copy()
        headers_analyse['Content-Type'] = 'application/json'
        response_analyse = requests.request(
            "POST",
            url_extended_analyze_report,
            headers=headers_analyse,
            json=analyse_params
        )
        assert response_analyse.status_code == 200, "There is a problem in posting the analysis"
        print("Posting to analysis report")


def monthly_analyse_api(device_id: str, from_date: str, to_date: str) -> None:
    """API for monthly analysis ADL

    Args:
        device_id (str): device id
        from_date (str): start date
        to_date (str): end date
    """
    querystring = {
        "deviceId": device_id,
        "from": from_date,
        "to": to_date
    }
    headers = {
        'x-api-key': api_key,
        'Content-Type': 'application/json'
    }
    response_get = requests.request(
        "GET",
        url_get_extended_analyze_report,
        headers=headers,
        params=querystring
    )
    public_key = enumOpcodeReadPubKeyConfig()

    df_cols = pd.DataFrame(columns=[
        "sleepDuration",
        "restlessness",
        "goToSleepTime",
        "wakeUpTime",
        "numberOfOutOfBedDuringNight",
        "durationOfOutOfBed",
        "sleepDurationDuringDay",
        "locationDistributionOfOutOfBedDuringNight",
        "averageNightlyRR",
        "locationDistributionDuringDay",
        "sedentaryDurationDuringDay",
        "aloneTime",
        "numberOfAcuteFalls",
        "numberOfModerateFalls",
        "numberOfLyingOnFloor",
        "gaitStatisticsDuringDay"
        ])
    df = pd.DataFrame([res['data'] for res in response_get.json()])
    df = pd.concat([df, df_cols])
    sleep_status, activity_status, alone_status, fall_status, acute_fall_status, total_status = monthly_adl.get_monthly_stats(df)

    monthly_adl_params = {
        "deviceId": device_id,
        "publicKey": public_key,
        "data": {
            "sleepDuration": sleep_status["night_sleep"],
            "restlessness": sleep_status["night_restless"],
            "goToSleepTime": sleep_status["go_to_sleep_time"],
            "wakeUpTime": sleep_status["wake_up_time"],
            "numberOfOutOfBedDuringNight": sleep_status["number_out_of_bed"],
            "durationOfOutOfBed": sleep_status["number_out_of_bed"],
            "sleepDurationDuringDay": sleep_status["day_sleep"],
            "locationDistributionOfOutOfBedDuringNight": sleep_status["location_dist"],
            "averageNightlyRR": sleep_status["respiration"],
            "locationDistributionDuringDay": activity_status["location_distribution"],
            "sedentaryDurationDuringDay": activity_status["sedentary"],
            "aloneTime": alone_status["alone_time"],
            "numberOfAcuteFalls": acute_fall_status,
            "numberOfModerateFalls": fall_status["moderate_fall"],
            "numberOfLyingOnFloor": fall_status["long_lying_on_floor"],
            "gaitStatisticsDuringDay": [
                activity_status["walking_total_distance"],
                activity_status["walking_speed"],
                activity_status["walking_sessions"],
                activity_status["walking_average_distance"]
            ],
            "analysis": {
                "sleepQuality": sleep_status["sleep_quality"],
                "activityLevel": activity_status["activity_level"],
                "aloneTime": alone_status["alone_time"],
                "fallRisk": fall_status["fall_risk"],
                "totalStatus": total_status
                }
        }
    }

    response_post = requests.request(
        "POST",
        url_extended_statistics_report,
        headers=headers,
        json=monthly_adl_params
    )
    assert response_post.status_code == 200, "There is a problem in posting the monthly statistics"
    print("Posting to monthly statistics report")


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-m', '--mode')
    parser.add_argument('-d', '--device_id')
    parser.add_argument('-f', '--from_date')
    parser.add_argument('-t', '--to_date')
    args = parser.parse_args()
    if args.mode == "day":
        daily_analyse_api(args.device_id, args.from_date, args.to_date)
    elif args.mode == "month":
        monthly_analyse_api(args.device_id, args.from_date, args.to_date)
    else:
        print("The mode needs to be: day/month")
