import requests


url = "https://backend-dev.echocare-ncs.com/api/device/getExtendedReport"

querystring = {
    "deviceId":"DemoRoom",
    "from":"2023-06-01",
    "to":"2023-06-17"
}

headers = {
    'x-api-key': "wH2JyNCYzeoxmdJdHlizvzVneyDB92B4yXOyPtTH4ulP07uWIPoUDiRY32i1ZKVwodGw6Ecgu1zEYmC0HElntLoPLp1J58bGwXcJ6VJgfYszi8BBOTHa6DBfg6qb2Dwi"
}


if __name__ == '__main__':

    response = requests.request("GET", url, headers=headers, params=querystring)

    print(response.text)

    print("finish")