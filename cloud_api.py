import json
import requests

DeviceIdFile = "./Certificates/Certificate/DeviceId.key"
publicKeyFile = "./Certificates/Certificate/publicKey.key"

def enumOpcodeReadPubKeyConfig ():
    with open( publicKeyFile  , 'r') as file:
        publicKey = file.read().replace('\n', '')

    print( publicKey )
    return publicKey


def enumOpcodeReadIdConfig ():

    with open( DeviceIdFile  , 'r') as file:
        DeviceId = file.read().replace('\n', '')

    print(DeviceId)
    return DeviceId

pubkey = enumOpcodeReadPubKeyConfig ()
deviceId = enumOpcodeReadIdConfig ()

url_extended_report     = "https://backend-dev.echocare-ncs.com/api/device/extendedReport"
url_get_extended_report = "https://backend-dev.echocare-ncs.com/api/device/getExtendedReport"

querystring = {
    "deviceId":"DemoRoom",
    "from":"2023-06-01",
    "to":"2023-06-30"
}

headers = {
    'x-api-key': "wH2JyNCYzeoxmdJdHlizvzVneyDB92B4yXOyPtTH4ulP07uWIPoUDiRY32i1ZKVwodGw6Ecgu1zEYmC0HElntLoPLp1J58bGwXcJ6VJgfYszi8BBOTHa6DBfg6qb2Dwi"
}

adl_data = {
    "deviceId": deviceId ,
    "publicKey": pubkey ,
    "data": {
        "sleepMonitoring": [
            {
                "sessionIndex": 1,
                "sessionStartTime": "2023-06-11T22:00:00Z",
                "sessionStopTime": "2023-06-12T06:00:00Z",
                "sessionRestless": 4
            },
            {
                "sessionIndex": 2,
                "sessionStartTime": "2023-06-12T22:00:00Z",
                "sessionStopTime": "2023-06-13T06:00:00Z",
                "sessionRestless": 3
            }
        ],
        "locations": {
            "objects": [
                {
                    "locationName": "Home",
                    "locationStartTime": "2023-06-12T07:00:00Z",
                    "locationStopTime": "2023-06-12T20:00:00Z"
                },
                {
                    "locationName": "Work",
                    "locationStartTime": "2023-06-13T08:00:00Z",
                    "locationStopTime": "2023-06-13T17:00:00Z"
                }
            ],
            "numbers": [5, 6, 7]
        },
        "respirations": [
            {
                "respirationRate": 15,
                "respirationTime": "2023-06-12T10:00:00Z"
            },
            {
                "respirationRate": 16,
                "respirationTime": "2023-06-12T20:00:00Z"
            }
        ],
        "gaitAnalysis": [
            {
                "numberOfWalkingSessions": 3,
                "totalWalkDistance": 5.2,
                "totalWalkDuration": 60
            }
        ]
    }
}


buffer = []
if __name__ == '__main__':

    enumOpcodeReadPubKeyConfig()

    enumOpcodeReadIdConfig()

    response = requests.request("POST", url_extended_report , headers=headers, data=adl_data)

    print(response.text)

    response = requests.request("GET", url_get_extended_report , headers=headers, params=querystring)

    print(response.text)

    print( "finish")