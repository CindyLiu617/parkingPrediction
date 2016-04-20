from datetime import *


def str_to_datetime(time_stamp, fmt, cache=None):
    if time_stamp == '' or time_stamp is None:
        return None

    if cache is not None:
        if time_stamp not in cache:
            cache[time_stamp] = datetime.strptime(time_stamp, fmt)
        return cache[time_stamp]
    else:
        return datetime.strptime(time_stamp, fmt)
