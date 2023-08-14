import requests
import json

DeviceIdFile = "./Certificates/Certificate/DeviceId.key"
publicKeyFile = "./Certificates/Certificate/publicKey.key"

def enumOpcodeReadPubKeyConfig():
    with open( publicKeyFile  , 'r') as file:
        publicKey = file.read().replace('\n', '')

    print(publicKey)
    return publicKey


def enumOpcodeReadIdConfig():

    with open(DeviceIdFile, 'r') as file:
        DeviceId = file.read().replace('\n', '')

    print(DeviceId)
    return DeviceId


url_extended_report     = "https://backend-dev.echocare-ncs.com/api/device/extendedReport"
url_get_extended_report = "https://backend-dev.echocare-ncs.com/api/device/getExtendedReport"

querystring = {
    "deviceId":"DemoRoom",
    "from":"2023-07-01",
    "to":"2023-07-31"
}

api_key = "wH2JyNCYzeoxmdJdHlizvzVneyDB92B4yXOyPtTH4ulP07uWIPoUDiRY32i1ZKVwodGw6Ecgu1zEYmC0HElntLoPLp1J58bGwXcJ6VJgfYszi8BBOTHa6DBfg6qb2Dwi"

headers = {
    'x-api-key': api_key  
}

headers_alerts = {
    'x-api-key': api_key,
    'Content-Type': 'application/json'
}

if __name__ == '__main__':

    pubkey = enumOpcodeReadPubKeyConfig()
    deviceId = "DemoRoom" # enumOpcodeReadIdConfig()


    adl_data = {
        "deviceId": deviceId ,
        "publicKey": pubkey ,
        "data": {
            "sleepMonitoring": [
                {
                    "sessionIndex": 1,
                    "sessionStartTime": "2023-07-11 22:00:00",
                    "sessionStopTime": "2023-07-11 22:45:00",
                    "sessionRestless": 4
                },
                {
                    "sessionIndex": 2,
                    "sessionStartTime": "2023-07-11 22:45:20",
                    "sessionStopTime": "2023-07-11 23:55:10",
                    "sessionRestless": 3
                },
                {
                    "sessionIndex": 3,
                    "sessionStartTime": "2023-07-11 23:55:17",
                    "sessionStopTime": "2023-07-12 00:30:00",
                    "sessionRestless": 3.5
                },
                {
                    "sessionIndex": 4,
                    "sessionStartTime": "2023-07-12 01:00:15",
                    "sessionStopTime": "2023-07-12 02:45:00",
                    "sessionRestless": 4
                },
                {
                    "sessionIndex": 5,
                    "sessionStartTime": "2023-07-12 03:00:15",
                    "sessionStopTime": "2023-07-12 03:45:00",
                    "sessionRestless": 3
                },
                {
                    "sessionIndex": 6,
                    "sessionStartTime": "2023-07-12 04:20:15",
                    "sessionStopTime": "2023-07-12 05:45:00",
                    "sessionRestless": 5
                },
                {
                    "sessionIndex": 7,
                    "sessionStartTime": "2023-07-12 05:45:15",
                    "sessionStopTime": "2023-07-12 07:05:00",
                    "sessionRestless": 4
                },
                {
                    "sessionIndex": 8,
                    "sessionStartTime": "2023-07-12 13:00:00",
                    "sessionStopTime": "2023-07-12 14:00:00",
                    "sessionRestless": 3
                },
                {
                    "sessionIndex": 9,
                    "sessionStartTime": "2023-07-12 16:00:00",
                    "sessionStopTime": "2023-07-12 16:40:00",
                    "sessionRestless": 4
                }
            ],
            "locations": {
                "objects": [
                    {
                        "locationName": "Kitchen",
                        "locationStartTime": "2023-07-11 16:00:00",
                        "locationStopTime": "2023-07-11 20:00:00"
                    },
                    {
                        "locationName": "Kitchen",
                        "locationStartTime": "2023-07-11 20:00:12",
                        "locationStopTime": "2023-07-11 22:00:00"
                    },
                    {
                        "locationName": "Bed",
                        "locationStartTime": "2023-07-11 22:00:00",
                        "locationStopTime": "2023-07-12 00:30:00"
                    },
                    {
                        "locationName": "Bedroom",
                        "locationStartTime": "2023-07-12 00:30:10",
                        "locationStopTime": "2023-07-12 01:00:00"
                    },
                    {
                        "locationName": "Bed",
                        "locationStartTime": "2023-07-12 01:00:00",
                        "locationStopTime": "2023-07-12 03:45:00"
                    },
                    {
                        "locationName": "Bathroom",
                        "locationStartTime": "2023-07-12 03:45:00",
                        "locationStopTime": "2023-07-12 04:00:00"
                    },
                    {
                        "locationName": "Kitchen",
                        "locationStartTime": "2023-07-12 04:00:00",
                        "locationStopTime": "2023-07-12 04:20:00"
                    },
                    {
                        "locationName": "Bed",
                        "locationStartTime": "2023-07-12 04:20:00",
                        "locationStopTime": "2023-07-12 08:00:00"
                    },
                    {
                        "locationName": "Bedroom",
                        "locationStartTime": "2023-07-12 08:00:00",
                        "locationStopTime": "2023-07-12 11:30:00"
                    },
                    {
                        "locationName": "Kitchen",
                        "locationStartTime": "2023-07-12 11:30:00",
                        "locationStopTime": "2023-07-12 13:00:00"
                    },
                    {
                        "locationName": "Bed",
                        "locationStartTime": "2023-07-12 13:00:00",
                        "locationStopTime": "2023-07-12 14:00:00"
                    }
                ],
                "numbers": [5, 6],
                "mapping": ["Home", "Work"]
            },
            "respirations": [
                {
                    "respirationRate": 15,
                    "respirationTime": "2023-07-12 01:00:00",
                    "heartRate": 30
                },
                {
                    "respirationRate": 16,
                    "respirationTime": "2023-07-12 04:00:00",
                    "heartRate": 40
                }
            ],
            "gaitAnalysis": [
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                },
                {
                    "numberOfWalkingSessions": 3,
                    "totalWalkDistance": 5.2,
                    "totalWalkDuration": 60,
                    "activityLevel": 1
                }
            ]
        }
    }


    response = requests.request("POST", url_extended_report , headers=headers, json=adl_data)

    print(response.text)

    print("finish")
