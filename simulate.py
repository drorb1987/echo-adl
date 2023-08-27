"""
Simulates data for sleep, visitors, gait and alerts
"""
import scipy.stats as stats
import pandas as pd
import numpy as np

location_default = ['Bed', 'Bedroom', 'Bathroom', 'Entry', 'OUT OF HOME']

def convert_to_date(dt):
    return dt.strftime("%a %b %d %H:%M:%S %Y")

class SimulateDailyData:
    """
    I think it is easiest to discretize times into some resolution (say 1 minute) and draw the
    start times from there. If you really want random start and ends you can add some jitter within the
    intervals
    First draw wake up and sleep times. For the night time, draw
    number of out of bed from a poisson distribution. Each out of bed during the night yields a location
    and a duration - the person will be in the given location for the entire duration and will then return
    to the bed (assume the system gives a sleep session immediately upon return to bed and ends it immediately upon
    getting up). During the sleep sessions draw number of respiration times and equally distribute them amongst
    sleep sessions (not too critical). For each sleep session draw restlessness from any non-zero distribution. Ignore gait
    from getting up during night for now (not used at the moment)

    During the day, first draw times when the person is asleep and the duration. During that time there
    is no walking and activity is low (location bed). Then use a markov chain to draw a probability of location
    transition over the discretized times (the sleep times fix a transition to bed at the start and a transition
    out of bed at the end). If there is a transition, then draw gait parameters (session, distance and speed) as well
    as the location being transitioned to. The gait will be added to the hourly count and the location will
    be transitioned to (for simplicity do it right at the beginning of the session).
    """
    def __init__(self, locations, night_location_distribution, day_location_distribution,
                 transition_matrix, date,
                 wake_up_time_average='08:00', sleep_time_average='22:00', gait_session_num_rate=1,
                 gait_session_speed=0.75, gait_session_distance=4, num_out_of_bed_rate=1,
                 out_of_bed_duration=0.2, num_respiration_rate=5, respiration_rate=15,
                 heart_rate=80, restlessness=0.05, sleep_during_day_duration=1,
                 sleep_during_day_rate=0.2, location_transition_prob=0.05, visitors_rate=0.3,
                 visitors_duration=1.5, time_res=5):
        self.locations = locations
        self.night_location_distribution = night_location_distribution
        self.day_location_distribution = day_location_distribution
        self.transition_matrix = transition_matrix
        self.wake_up_time_av = wake_up_time_average
        self.sleep_time_av = sleep_time_average
        self.gait_session_num_rate = gait_session_num_rate
        self.gait_session_speed = gait_session_speed
        self.gait_session_distance = gait_session_distance
        self.num_out_of_bed_rate = num_out_of_bed_rate
        self.out_of_bed_duration = out_of_bed_duration
        self.num_respiration_rate = num_respiration_rate
        self.respiration_rate = respiration_rate
        self.heart_rate = heart_rate
        self.restlessness = restlessness
        self.sleep_during_day_duration = sleep_during_day_duration
        self.sleep_during_day_rate = sleep_during_day_rate
        self.location_transition_prob = location_transition_prob
        self.visitors_rate = visitors_rate
        self.visitors_duration = visitors_duration
        # minutes
        self.time_res = time_res
        self.date = date
        self.date_format = convert_to_date

    def simulate(self):
        # draw wake up and sleep time offsets
        wake_up_time = pd.to_datetime(self.wake_up_time_av) + pd.to_timedelta(stats.norm(0, 0.33).rvs(), 'h')
        sleep_time = pd.to_datetime(self.sleep_time_av) + pd.to_timedelta(stats.norm(0, 0.33).rvs(), 'h')
        # simulate sleep - returns dataframe of sleep sessions and locations
        df_sleep, df_locations, df_respirations = self.simulate_sleep(wake_up_time, sleep_time)

    def simulate_sleep(self, wake_up_time, sleep_time):
        # discretize time between sleep and wake up
        # leave an hour from first sleep time and an hour from wake up time
        session_start = []
        session_end = []
        restlessness = []
        location_start = []
        location_end = []
        locations = []

        stop = pd.to_datetime(self.date + ' ' + wake_up_time.strftime("%H:%M:%S")) + pd.to_timedelta(1, 'd')
        # assume it's a full day
        start = pd.to_datetime(self.date + ' ' + sleep_time.strftime("%H:%M:%S"))

        times = pd.date_range(start, stop, freq=str(self.time_res) + 'min')
        start_index = int(60/self.time_res)
        sampling_indices = {i for i in range(start_index, len(times) - start_index)}
        # zero means sleep
        sleep_hours = np.zeros(len(times))
        location_hours = np.zeros(len(times))

        # draw out of bed cases
        num_out_of_bed = stats.poisson(self.num_out_of_bed_rate).rvs()
        gamma_scale = 10
        for event in range(num_out_of_bed):
            # draw the time
            awake_start = int(np.random.choice(list(sampling_indices), 1))
            awake_duration = stats.gamma(self.out_of_bed_duration*gamma_scale, scale=1/gamma_scale).rvs()
            # round up to get the number of indices that are out of bed
            num_indices = int(np.ceil(awake_duration*60/self.time_res))
            # draw the location
            loc = stats.multinomial(1, self.night_location_distribution).rvs().argmax()
            sleep_hours[awake_start: awake_start + num_indices + 1] = 1
            location_hours[awake_start: awake_start + num_indices + 1] = loc
            sampling_indices = sampling_indices - {i for i in range(awake_start, awake_start + num_indices + 1)}

        # draw respirations from sleep indices
        sleep_indices = sleep_hours == 0
        respiration_count = stats.poisson(self.num_respiration_rate).rvs()
        respiration_times = np.random.choice(sleep_indices, respiration_count, replace=False)
        respiration_values = stats.norm(self.respiration_rate, 2).rvs(respiration_count)
        hr_values = stats.norm(self.heart_rate, 3).rvs(respiration_count)

        # segment array to sleep and awake
        state = 0
        session_start.append(times[0])
        beta_ratio = (1 - self.restlessness)/self.restlessness
        alpha = 2
        beta = beta_ratio*alpha
        for i, elem in enumerate(sleep_hours):
            if elem != state:
                if state == 0:
                    session_end.append(i)
                    location_start.append(i)
                    # at the end of sleep draw restlessness
                    restlessness.append(stats.beta(alpha, beta).rvs())
                    locations.append(location_hours[i])
                else:
                    session_start.append(i)
                    location_end.append(i)
                state = elem

        df_sleep = pd.DataFrame({'start': self.date_format(times[session_start]),
                                 'stop': self.date_format(times[session_end]),
                                 'restlessness': restlessness})
        df_locations = pd.DataFrame({'start': self.date_format(times[location_start]),
                                     'stop': self.date_format(times[location_end]),
                                     'location': self.locations[locations]})

        df_respiration = pd.DataFrame({'time': self.date_format(respiration_times),
                                       'rr': respiration_values,
                                       'hr': hr_values})

        return df_sleep, df_locations, df_respiration



if __name__ == "__main__":
    # we'll return the day in correct format for each dataframe
    SimulateDailyData(location_default, [0, 0.4, 0.5, 0, 0.1], None, None, 'Sun Jun 11 2023').simulate()

