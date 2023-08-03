import pandas as pd
import json

GREEN = 0
YELLOW = 1
YELLOW_UP = 2
YELLOW_DOWN = 3
RED = 4
RED_UP = 5
RED_DOWN = 6


def get_parameters() -> dict:
    json_file = './parameters.json'
    with open(json_file, "r") as f:
        parameters = json.load(f)
    return parameters


def get_total_status(d: dict) -> int:
    """Get total status for the measurement

    Args:
        d (dict): a dictionary of all the stats of the measurement

    Returns:
        int: the color to indicate the status of the measurement
    """
    statuses = [d[key] for key in d]
    status = GREEN
    if RED in statuses or RED_UP in statuses or RED_DOWN in statuses:
        status = RED
    elif YELLOW in statuses or YELLOW_DOWN in statuses or YELLOW_UP in statuses:
        status = YELLOW
    return int(status)


def calc_statistics(analyse_param: pd.Series) -> int:
    """Calculate the status of the parameter based on the mean and std.

    Args:
        analyse_param (pd.Series): The measurement parameter that need to be analysed

    Returns:
        int: status of the parameter
    """
    parameters = get_parameters()
    std_upper_th = parameters["Thresholds"]["STD_UPPER_TH"]
    std_lower_th = parameters["Thresholds"]["STD_LOWER_TH"]
    mean = analyse_param.mean()
    std = analyse_param.std()
    status = GREEN
    if not std or pd.isna(std):
        return status
    if any(analyse_param.rolling(2).apply(lambda x: all(x > mean+std_upper_th*std)).dropna()):
        status = RED_UP
    elif any(analyse_param.rolling(3).apply(lambda x: all((mean+std_lower_th*std < x) & (x < mean+std_upper_th*std))).dropna()):
        status = YELLOW_UP
    elif any(analyse_param.rolling(2).apply(lambda x: all(x < mean-std_upper_th*std)).dropna()):
        status = RED_DOWN
    elif any(analyse_param.rolling(3).apply(lambda x: all((mean-std_lower_th*std > x) & (x > mean-std_upper_th*std))).dropna()):
        status = YELLOW_DOWN
    return int(status)


def calc_statistics_by_number(analyse_param: pd.Series) -> int:
    """Calculate the status of the measurement parameter based on number

    Args:
        analyse_param (pd.Series): The measurement parameter that need to be analysed

    Returns:
        int: status of the parameter
    """
    parameters = get_parameters()
    num_th = parameters["Thresholds"]["NUM_TH"]
    status = GREEN
    number = analyse_param[-10:].sum()
    if 0 < number < num_th:
        status = YELLOW
    elif number >= num_th:
        status = RED
    return int(status)


def calc_location_distribution(analyse_param: pd.Series) -> dict[str, int]:
    """Calculate the locations distribution over a month

    Args:
        analyse_param (pd.Series): a pandas series that contains the daily locations distribution.

    Returns:
        dict[str, int]: a dictionary of the the monthly locations distribution.
    """
    data = [d for d in analyse_param.to_list() if d and not pd.isna(d)]
    d = dict(pd.DataFrame(data).sum())
    return {key: int(val) for key, val in d.items()}


def acute_fall(analyse: pd.Series) -> bool:
    """Detect if there is an acute fall

    Args:
        analyse (pd.Series): a pandas series contains the number of the acute falls per day

    Returns:
        bool: a flag for detection an acute fall
    """
    if analyse.sum() > 0:
        return True
    return False


def sleep_quality(analyse: pd.DataFrame) -> dict[str, int]:
    """Calculate the sleep quality

    Args:
        analyse (pd.DataFrame): a data frame contains the daily data from the cloud API

    Returns:
        dict: a dictionary that have all the sleep quality statuses
    """
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
    """Calculate the activity level

    Args:
        analyse (pd.DataFrame): a data frame contains the daily data from the cloud API

    Returns:
        dict: a dictionary that have all the activity level statuses
    """
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
    """Calculate the alone time

    Args:
        analyse (pd.DataFrame): a data frame contains the daily data from the cloud API

    Returns:
        dict: a dictionary that have all the alone time statuses
    """
    quality = {}
    quality['alone_time'] = calc_statistics(analyse["aloneTime"])
    return quality


def fall_risk(analyse: pd.DataFrame) -> dict:
    """Calculate the fall risk

    Args:
        analyse (pd.DataFrame): a data frame contains the daily data from the cloud API

    Returns:
        dict: a dictionary that have all the fall risk statuses
    """
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


def get_monthly_stats(analyse: pd.DataFrame) -> tuple[dict, dict, dict, dict, bool, int]:
    """Get the monthly statuses of all the measurements

    Args:
        analyse (pd.DataFrame): a data frame contains the daily data from the cloud API

    Returns:
        tuple[dict, dict, dict, dict, bool, int]: all the statuses for all the measurements and the acute fall flag
    """
    sleep_status = sleep_quality(analyse)
    activity_status = activity_level(analyse)
    alone_status = alone_time(analyse)
    fall_status = fall_risk(analyse)
    acute_fall_status = acute_fall(analyse["numberOfAcuteFalls"])
    total_dict = {
        'sleep_quality': sleep_status['sleep_quality'],
        'activity_level': activity_status['activity_level'],
        'alone_time': alone_status['alone_time'],
        'fall_risk': fall_status['fall_risk']
    }
    total_status = get_total_status(total_dict)
    return sleep_status, activity_status, alone_status, fall_status, acute_fall_status, total_status
