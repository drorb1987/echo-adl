import pandas as pd
from typing import Any

RED = 0
YELLOW = 1
GREEN = 2


def get_total_status(d: dict) -> float:
    statuses = [d[key] for key in d]
    if RED in statuses:
        return RED
    elif YELLOW in statuses:
        return YELLOW
    else:
        return GREEN


def calc_status_per(mean: float, std: float) -> int:
    status = GREEN

    return status


def calc_dev_status_per(deviations: list[float]) -> int:
    if all([dev <= 0.1 for dev in deviations]):
        status = GREEN
    elif deviations <= 0.5:
        status = YELLOW
    else:
        status = RED
    return status


def calc_statistics(analyse_param: pd.Series) -> float:
    mean = analyse_param.dropna().apply('average')
    std = analyse_param.apply('std')
    status = GREEN
    if any(analyse_param.rolling(2).apply(lambda x: all(x > mean+2*std))):
        status = RED
    elif any(analyse_param.rolling(3).apply(lambda x: all(mean+2*std > x > mean+std))):
        status = YELLOW
    return status
    # deviation = abs(analyse_param - mean) / mean
    # status = calc_dev_status_per(deviation)
    # return {'all': analyse_param.to_list(), 'mean': mean, 'std': std, 'deviation': deviation.to_list(), 'status': status}


def calc_location_distribution(analyse_param: pd.Series):
    return dict(pd.DataFrame(analyse_param.to_list()).sum())


def sleep_quality(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['night_sleep_stats'] = calc_statistics(analyse["sleepDurationDuringNight"])
    quality['night_restless_stats'] = calc_statistics(analyse["restlessnessDuringNight"])
    quality['go_to_sleep_time_stats'] = calc_statistics(analyse["goToSleepTime"])
    quality['wake_up_time_stats'] = calc_statistics(analyse["wakeUpTime"])
    quality['number_out_of_bed_stats'] = calc_statistics(analyse["outOfBedDuringNight"])
    quality['duration_out_of_bed_stats'] = calc_statistics(analyse["durationOfOutOfBed"])
    quality['day_sleep_stats'] = calc_statistics(analyse["sleepDurationDuringDay"])
    quality['location_dist_stats'] = calc_statistics(analyse["locationDistributionOfOutOfBedDuringNight"])
    quality['respiration_stats'] = calc_statistics(analyse["averageNightlyRR"])
    quality['sleep_quality'] = get_total_status(quality)
    return quality
    

def activity_level(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['sedantry_stats'] = calc_statistics(analyse["sedentaryDurationDuringDay"])
    quality['location_distribution_stats'] = calc_location_distribution(analyse["locationDistributionDuringDay"])
    gait_df = pd.DataFrame(analyse["gaitStatisticsDuringDay"].to_list())
    quality['walking_distance_stats'] = calc_statistics(gait_df[2])
    quality['walking_speed_stats'] = calc_statistics(gait_df[2]/gait_df[1])
    quality['walking_sessions_stats'] = calc_statistics(gait_df[0])
    # walking_distance_per_session
    quality['activity_level'] = get_total_status(quality)
    return quality


def alone_time(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['alone_time'] = calc_statistics(analyse["aloneTime"])
    return quality


def fall_risk(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['acute_fall_stats'] = calc_statistics(analyse["acuteFalls"])
    quality['moderate_fall_stats'] = calc_statistics(analyse["moderateFalls"])
    quality['long_lying_on_floor_stats'] = calc_statistics(analyse["lyingOnFloor"])
    quality['sedantry_stats'] = calc_statistics(analyse["sedentaryDurationDuringDay"])
    quality['night_restless_stats'] = calc_statistics(analyse["restlessnessDuringNight"])
    quality['number_out_of_bed_stats'] = calc_statistics(analyse["outOfBedDuringNight"])
    gait_df = pd.DataFrame(analyse["gaitStatisticsDuringDay"].to_list())
    quality['walking_speed_stats'] = calc_statistics(gait_df[2]/gait_df[1])
    quality['fall_risk'] = get_total_status(quality)


def get_monthly_stats(analyse: pd.DataFrame) -> tuple[dict, dict, dict, dict]:
    sleep_status = sleep_quality(analyse)
    activity_status = activity_level(analyse)
    alone_status = alone_time(analyse)
    fall_status = fall_risk(analyse)
    return sleep_status, activity_status, alone_status, fall_status
