from datetime import *


def str_to_datetime(time_stamp, fmt):
    if time_stamp == '' or time_stamp is None:
        return None
    return datetime.strptime(time_stamp, fmt)
