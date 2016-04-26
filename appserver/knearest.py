import json
from datetime import date, datetime, timedelta
import Queue
import math

from utils import str_to_datetime
from database import DBTable
from database import DBRecord

PREDICTION_TABLE = 'prediction'
HISTORICAL_TABLE = 'carOccupancy'
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S'
DATA_STRUCTURE = ['YEAR', 'MONTH', 'WEEK', 'WEEKDAY', 'HOUR']
my_db = DBTable()
my_predict_table = DBRecord(PREDICTION_TABLE)
my_history_table = DBRecord(HISTORICAL_TABLE)


def quote(val):
    return '\'%s\'' % str(val)


# delta should be like timedelta(hours = 1)
# start and end should be in datetime.strptime(s, "%Y-%m-%d  %H:%M:%S") format
def get_span(start, end, delta):
    start_date = str_to_datetime(start, DATE_FORMAT)
    end_date = str_to_datetime(end, DATE_FORMAT)
    cur_date = start_date
    while cur_date < end_date:
        yield cur_date
        cur_date += delta


def k_nearest(start, end):
    col_list = []
    earliest_history = []
    latest_history = []
    with open(
            '/Users/James/Desktop/PP/Learning/BitTiger/ParkingPrediction/parkingPrediction/appserver/database/PredictionDBMetaSchema.json') as data_file:
        data = json.load(data_file)
        if my_db.is_valid_table(PREDICTION_TABLE):
            my_db.remove_table(PREDICTION_TABLE)
        my_db.create_table(PREDICTION_TABLE)
        for col_schema in data['metaSchema']['cols']:
            col_name = data['colsData'][col_schema]['name']
            col_type = data['colsData'][col_schema]['type']
            my_db.insert_col(PREDICTION_TABLE, col_name, col_type)
            col_list.append(col_name.encode('utf-8'))

        earliest_year = my_predict_table.get_min_in_col('YEAR')
        latest_year = my_predict_table.get_max_in_col('YEAR')
        earliest_history.append(earliest_year[0][0])
        latest_history.append(latest_year[0][0])

        earliest_month = my_predict_table.get_min_in_col('MONTH')
        latest_month = my_predict_table.get_max_in_col('MONTH')
        earliest_history.append(earliest_month[0][0])
        latest_history.append(latest_month[0][0])

        earliest_weekday = my_predict_table.get_min_in_col('WEEKDAY')
        latest_weekday = my_predict_table.get_max_in_col('WEEKDAY')
        earliest_history.append(earliest_weekday[0][0])
        latest_history.append(latest_weekday[0][0])

        earliest_week = my_predict_table.get_min_in_col('WEEK')
        latest_week = my_predict_table.get_max_in_col('WEEK')
        earliest_history.append(earliest_week[0][0])
        latest_history.append(latest_week[0][0])

        earliest_hour = my_predict_table.get_min_in_col('HOUR')
        latest_hour = my_predict_table.get_max_in_col('HOUR')
        earliest_history.append(earliest_hour[0][0])
        latest_history.append(latest_hour[0][0])

        for every_hour in get_span(start, end, timedelta(hours=1)):
            time_tuple = every_hour.timetuple()
            year = time_tuple[0]
            month = time_tuple[1]
            weekday = time_tuple[6]
            week = time_tuple[2] / 7
            hour = time_tuple[3]

            prediction = [year, month, week, weekday, hour]
            knns = _get_knns(prediction)
            predict_count = _get_estimated(knns)
            my_predict_table.insert(col_list,
                                    [_quote(year), _quote(month), _quote(week), _quote(weekday), _quote(hour),
                                     _quote(predict_count)])
        my_predict_table.commit_record()
        my_db.add_index(PREDICTION_TABLE, 'query_index', col_list[:-1])
        my_db.close()


def _quote(val):
    return '\'%s\'' % str(val)


def _yield_neighbours(origin):
    i_start = _find_first_nz(origin)
    for i in range(i_start, len(origin)):
        if origin[i] >= 0:
            new = origin[:]
            new[i] += 1
            yield new[:]
        if origin[i] <= 0:
            new = origin[:]
            new[i] -= 1
            yield new[:]


def _get_knns(prediction, k=5, max_itr=50):
    my_q = Queue.Queue()
    my_q.put(([0, 0, 0, 0, 0], 0))
    nearest_neighbours = []
    itr_cnt = 0
    while my_q and itr_cnt < max_itr:
        cur = my_q.get()
        delta = cur[0]
        distance = cur[1]
        neighbour = _vector_addition(delta, prediction)
        if distance > 0 and _is_valid(DATA_STRUCTURE, neighbour):
            # query car count
            car_count_set = my_history_table.get_record('CAR_NUM', DATA_STRUCTURE, neighbour)
            nearest_neighbours.append((car_count_set[0], distance))
        if len(nearest_neighbours) == k:
            break
        for element in _yield_neighbours(delta):
            my_q.put((element, distance + 1))
        itr_cnt += 1
    return nearest_neighbours


def _find_first_nz(cur):
    for i in range(len(cur)):
        if cur[i] != 0:
            return i
    return 0


# cur is neighbour list, for example cur is [1, 0, 0, 0, 0], data_need_to_predict is [2013, 1, 4, 3, 5]
# then get_data = [2014, 1, 4, 3, 5]
def _vector_addition(lhs, rhs):
    res = [0] * len(lhs)
    for i in range(len(lhs)):
        res[i] = lhs[i] + rhs[i]
    return res


def _is_valid(col_list, attr_values):
    return my_history_table.is_existing(col_list, attr_values)


# get prediction car number
def _get_estimated(num_dist_pairs, sigma=10.0):
    avg = 0
    total_weight = 0
    for pair in num_dist_pairs:
        weight = math.e ** (-pair[1] ** 2 / (2 * sigma ** 2))
        avg += pair[0] * weight
        total_weight += weight
    return avg / total_weight
