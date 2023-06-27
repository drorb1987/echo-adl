import requests
import pandas as pd
import adl

DeviceIdFile = "./Certificates/Certificate/DeviceId.key"
publicKeyFile = "./Certificates/Certificate/publicKey.key"

url_extended_report     = "https://backend-dev.echocare-ncs.com/api/device/extendedReport"
url_get_extended_report = "https://backend-dev.echocare-ncs.com/api/device/getExtendedReport"

url_extended_analyze_report = "https://backend-dev.echocare-ncs.com/api/device/setAnalyzedReport"
url_get_emergenies_report = "https://backend-dev.echocare-ncs.com/api/device/getEmergencies"

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
    sleep_mapper = {
        'sessionStartTime': 'start',
        'sessionStopTime': 'stop',
        'sessionRestless': 'restless'
    }
    sleep_columns = ['start', 'stop', 'restless']
    sleep_df = pd.DataFrame(res['data']['sleepMonitoring']).rename(columns=sleep_mapper)
    return sleep_df[sleep_columns]
    
def warp_location_df(res: dict) -> pd.DataFrame:
    location_mapper = {
        'locationStartTime': 'start',
        'locationStopTime': 'stop',
        'locationName': 'location'
    }
    location_columns = ['start', 'stop', 'location']
    location_df = pd.DataFrame(res['data']['locations']['objects']).rename(columns=location_mapper)
    return location_df[location_columns]

def warp_respiration_df(res: dict) -> pd.DataFrame:
    respiration_mapper = {
        'respirationTime': 'time',
        'respirationRate': 'respiration',
        'heartRate': 'heart_rate'
    }
    respiration_columns = ['time', 'respiration', 'heart_rate']
    respiration_df = pd.DataFrame(res['data']['respirations']).rename(columns=respiration_mapper)
    return respiration_df[respiration_columns]

def warp_gait_df(res: dict) -> pd.DataFrame:
    gait_mapper = {
        'numberOfWalkingSessions': 'number_of_sessions',
        'totalWalkDistance': 'total_distance',
        'totalWalkDuration': 'total_time',
        'activityLevel': 'activity'
    }
    gait_columns = ['number_of_sessions', 'total_distance', 'total_time', 'activity']
    gait_df = pd.DataFrame(res['data']['gaitAnalysis']).rename(columns=gait_mapper)
    return gait_df[gait_columns]

def get_cloud_api(device_id: str, time_from: str, time_to: str):
    querystring = {
        "deviceId": device_id,
        "from": time_from,
        "to": time_to
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
    
    analyse_body = []

    for res in response.json():
        sleep_df = warp_sleep_df(res)
        sleep_df, time_dict = adl.get_day_night_times(sleep_df)
        night_sleep_duration = adl.get_sleep_duration(sleep_df, time_dict, 'Night')
        day_sleep_duration = adl.get_sleep_duration(sleep_df, time_dict, 'Day')
        night_restlessness = adl.get_restlessness(sleep_df, time_dict, 'Night')
        day_restlessness = adl.get_restlessness(sleep_df, time_dict, 'Day')
        # sleep_df.rename(columns=inverse_sleep_mapper, inplace=True)

        # location
        location_df = warp_location_df(res)
        location_df = adl.create_consecutive_df(location_df)
        number_out_of_bed, night_out_of_bed_duration, location_counter = adl.count_out_of_bed_loaction_sleep(sleep_df, location_df, time_dict, 'Night')
        daily_location_distribution = adl.get_location_distribution(location_df, time_dict, 'Day', bed_included=True)

        respiration_df = warp_respiration_df(res)
        average_respiration = adl.get_average_respiration(respiration_df, time_dict, 'Night')
        average_heartrate = adl.get_average_heartrate(respiration_df, time_dict, 'Night')

        analyse_params = {
            "deviceID": device_id,
            "goToSleepTime": time_dict["Night"]["start"],
            "wakUpTime": time_dict["Night"]["stop"],
            "sleepDurationDuringDay": day_sleep_duration,
            "sleepDurationDuringNight": night_sleep_duration,
            "restlessnessDuringDay": day_restlessness,
            "restlessnessDuringNight": night_restlessness,

            "outOfBedDuringNight": number_out_of_bed,
            "durationOfOutOfBed": night_out_of_bed_duration,
            "locationDistributionOfOutOfBedDuringNight": location_counter, # [1, 0, 0],

            "averageNightlyRR": average_respiration,
            "averageNightlyHR": average_heartrate,
            "locationDistributionDuringDay": daily_location_distribution, # [5, 3, 2],
            "sedentaryDurationDuringDay": 5,
            "aloneTime": 2,
            "acuteFalls": 0,
            "moderateFalls": 0,
            "lyingOnFloor": 0,
            "gaitStatisticsDuringDay": [2, 5, 3]
        }

        analyse_body.append(analyse_params)

def alerts_cloud_api():
    pass

if __name__ == '__main__':
    device_id = "DemoRoom"
    from_time = "2023-06-01"
    to_time = "2023-06-30"
    get_cloud_api(device_id, from_time, to_time)