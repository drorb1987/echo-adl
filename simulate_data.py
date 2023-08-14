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

    data_file = "./data.json"
    with open(data_file, 'r') as f:
        data = json.load(f)

    adl_data = {
        "deviceId": deviceId,
        "publicKey": pubkey,
        "data": data
    }

    response = requests.request("POST", url_extended_report , headers=headers, json=adl_data)

    print(response.text)

    print("finish")
