import pandas as pd
from typing import Any

RED = 0
YELLOW = 1
GREEN = 2


def get_total_status(d: dict) -> int:
    statuses = [d[key] for key in d]
    status = GREEN
    if RED in statuses:
        status = RED
    elif YELLOW in statuses:
        status = YELLOW
    return int(status)


def calc_statistics(analyse_param: pd.Series) -> int:
    mean = analyse_param.mean()
    std = analyse_param.std()
    status = GREEN
    if not std or pd.isna(std):
        return status
    if any(analyse_param.rolling(2).apply(lambda x: all(x > mean+2*std)).dropna()):
        status = RED
    elif any(analyse_param.rolling(3).apply(lambda x: all((mean+std < x) & (x < mean+2*std))).dropna()):
        status = YELLOW
    return int(status)


def calc_statistics_by_number(analyse_param: pd.Series) -> int:
    status = GREEN
    number = analyse_param[-10:].sum()
    if 0 < number < 5:
        status = YELLOW
    elif number >= 5:
        status = RED
    return int(status)


def calc_location_distribution(analyse_param: pd.Series) -> dict[str, int]:
    data = [d for d in analyse_param.to_list() if d and not pd.isna(d)]
    d = dict(pd.DataFrame(data).sum())
    return {key: int(val) for key, val in d.items()}


def acute_fall(analyse: pd.Series) -> bool:
    if analyse.sum() > 0:
        return True
    return False


def sleep_quality(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['night_sleep'] = calc_statistics(analyse["sleepDuration"])
    quality['night_restless'] = calc_statistics(analyse["restlessness"])
    quality['go_to_sleep_time'] = calc_statistics(analyse["goToSleepTime"].apply(pd.to_datetime))
    quality['wake_up_time'] = calc_statistics(analyse["wakeUpTime"].apply(pd.to_datetime))
    quality['number_out_of_bed'] = calc_statistics(analyse["numberOfOutOfBedDuringNight"])
    quality['duration_out_of_bed'] = calc_statistics(analyse["durationOfOutOfBed"])
    quality['day_sleep'] = calc_statistics(analyse["sleepDurationDuringDay"])
    quality['location_dist'] = calc_location_distribution(analyse["locationDistributionOfOutOfBedDuringNight"])
    quality['respiration'] = calc_statistics(analyse["averageNightlyRR"])
    quality['sleep_quality'] = get_total_status(quality)
    return quality
    

def activity_level(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['sedentary'] = calc_statistics(analyse["sedentaryDurationDuringDay"])
    quality['location_distribution'] = calc_location_distribution(analyse["locationDistributionDuringDay"])
    gait_df = pd.DataFrame(analyse["gaitStatisticsDuringDay"].to_list())
    quality['walking_total_distance'] = calc_statistics(gait_df[0])
    quality['walking_speed'] = calc_statistics(gait_df[1])
    quality['walking_sessions'] = calc_statistics(gait_df[2])
    quality['walking_average_distance'] = calc_statistics(gait_df[3])
    # walking_distance_per_session
    quality['activity_level'] = get_total_status(quality)
    return quality


def alone_time(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['alone_time'] = calc_statistics(analyse["aloneTime"])
    return quality


def fall_risk(analyse: pd.DataFrame) -> dict:
    quality = {}
    quality['acute_fall'] = acute_fall(analyse["numberOfAcuteFalls"])
    quality['moderate_fall'] = calc_statistics_by_number(analyse["numberOfModerateFalls"])
    quality['long_lying_on_floor'] = calc_statistics_by_number(analyse["numberOfLyingOnFloor"])
    quality['sedantry'] = calc_statistics(analyse["sedentaryDurationDuringDay"])
    quality['night_restless'] = calc_statistics(analyse["restlessness"])
    quality['number_out_of_bed'] = calc_statistics(analyse["numberOfOutOfBedDuringNight"])
    gait_df = pd.DataFrame(analyse["gaitStatisticsDuringDay"].to_list())
    quality['walking_speed'] = calc_statistics(gait_df[2]/gait_df[1])
    quality['fall_risk'] = get_total_status(quality)
    return quality


def get_monthly_stats(analyse: pd.DataFrame) -> tuple[dict, dict, dict, dict, bool]:
    sleep_status = sleep_quality(analyse)
    activity_status = activity_level(analyse)
    alone_status = alone_time(analyse)
    fall_status = fall_risk(analyse)
    acute_fall_status = acute_fall(analyse["numberOfAcuteFalls"])
    return sleep_status, activity_status, alone_status, fall_status, acute_fall_status
