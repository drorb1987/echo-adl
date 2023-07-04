import pandas as pd

RED = 0
YELLOW = 1
GREEN = 2


def get_total_status(*args):
    statuses = [stats['status'] for stats in args]
    if RED in statuses:
        return RED
    elif YELLOW in statuses:
        return YELLOW
    else:
        return GREEN


def calc_status_per(mean: float, std: float) -> int:
    normalized_std = std / mean
    if normalized_std <= 0.1:
        status = GREEN
    elif normalized_std <= 0.5:
        status = YELLOW
    else:
        status = RED
    return status


def calc_statistics(analyse_param: pd.Series) -> dict[str, float]:
    mean = analyse_param.apply('average')
    std = analyse_param.apply('std')
    status = calc_status_per(mean, std)
    return {'mean': mean, 'std': std, 'status': status}


def calc_location_distribution(analyse_param: pd.Series):
    return dict(pd.DataFrame(analyse_param.to_list()).sum())


def sleep_quality(analyse: pd.DataFrame) -> int:
    night_sleep_stats = calc_statistics(analyse["sleepDurationDuringNight"])
    night_restless_stats = calc_statistics(analyse["restlessnessDuringNight"])
    go_to_sleep_time_stats = calc_statistics(analyse["goToSleepTime"])
    wake_up_time_stats = calc_statistics(analyse["wakeUpTime"])
    number_out_of_bed_stats = calc_statistics(analyse["outOfBedDuringNight"])
    duration_out_of_bed_stats = calc_statistics(analyse["durationOfOutOfBed"])
    day_sleep_stats = calc_statistics(analyse["sleepDurationDuringDay"])
    location_dist_stats = calc_statistics(analyse["locationDistributionOfOutOfBedDuringNight"])
    respiration_stats = calc_statistics(analyse["averageNightlyRR"])
    return get_total_status(night_sleep_stats,
                            night_restless_stats,
                            go_to_sleep_time_stats,
                            wake_up_time_stats,
                            number_out_of_bed_stats,
                            duration_out_of_bed_stats,
                            day_sleep_stats,
                            location_dist_stats,
                            respiration_stats
                            )
    

def activity_level(analyse: pd.DataFrame) -> int:
    sedantry_stats = calc_statistics(analyse["sedentaryDurationDuringDay"])
    location_distribution_stats = calc_location_distribution(analyse["locationDistributionDuringDay"])
    gait_df = pd.DataFrame(analyse["gaitStatisticsDuringDay"].to_list())
    walking_distance_stats = calc_statistics(gait_df[2])
    walking_speed_stats = calc_statistics(gait_df[2]/gait_df[1])
    walking_sessions_stats = calc_statistics(gait_df[0])
    # walking_distance_per_session
    return get_total_status(
        sedantry_stats,
        location_distribution_stats,
        walking_distance_stats,
        walking_speed_stats,
        walking_sessions_stats 
    )


def alone_time(analyse: pd.DataFrame) -> int:
    alone_stats = calc_statistics(analyse["aloneTime"])
    return alone_stats['status']


def fall_risk(analyse: pd.DataFrame) -> int:
    acute_fall_stats = calc_statistics(analyse["acuteFalls"])
    moderate_fall_stats = calc_statistics(analyse["moderateFalls"])
    long_lying_on_floor_stats = calc_statistics(analyse["lyingOnFloor"])
    sedantry_stats = calc_statistics(analyse["sedentaryDurationDuringDay"])
    night_restless_stats = calc_statistics(analyse["restlessnessDuringNight"])
    number_out_of_bed_stats = calc_statistics(analyse["outOfBedDuringNight"])
    gait_df = pd.DataFrame(analyse["gaitStatisticsDuringDay"].to_list())
    walking_speed_stats = calc_statistics(gait_df[2]/gait_df[1])
    return get_total_status(
        acute_fall_stats,
        moderate_fall_stats,
        long_lying_on_floor_stats,
        sedantry_stats,
        night_restless_stats,
        number_out_of_bed_stats,
        walking_speed_stats
    )


def get_monthly_stats(analyse: pd.DataFrame) -> tuple[int, int, int, int]:
    sleep_status = sleep_quality(analyse)
    activity_status = activity_level(analyse)
    alone_status = alone_time(analyse)
    fall_status = fall_risk(analyse)
    return sleep_status, activity_status, alone_status, fall_status
