import pandas as pd
from adl_model import ADL_model

    
if __name__ == '__main__':
    adl_model = ADL_model()
    sleep_sessions = {
    'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
    'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
    'restless': [0.8, 0.4, 0.9, 0.2]
    }
    adl_model.set_sleep_df(sleep_sessions)

    location_data = {
        'start': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'stop': ['2023-01-05 11:00:00', '2023-01-05 18:00:00', '2023-01-06 07:15:00', '2023-01-06 19:00:00'],
        'location': ['Kitchen', 'Bed', 'Bedroom', 'Entry']
    }
    adl_model.set_location_df(location_data)

    respiration_data = {
        'time': ['2023-01-05 10:30:00', '2023-01-05 15:45:00', '2023-01-05 22:15:00', '2023-01-06 18:20:00'],
        'respiration': [30.2, 50.0, 45.1, 37.6]
    }
    adl_model.set_respiration_df(respiration_data)
