import json
from datetime import date, datetime, timedelta

import math

import sys

from utils import str_to_datetime
from database import DBTable
from database import DBRecord

PREDICTION_TABLE = 'prediction'
HISTORICAL_TABLE = 'carOccupancy'
DATE_FORMAT = '%Y-%m-%d  %H:%M:%S'
EARLIEST = [2013, 1, 1, 0, 1]
LATEST = [2016, 12, 31, 23, 7]

my_db = DBTable()
my_predict_table = DBRecord(PREDICTION_TABLE)
my_history_table = DBRecord(HISTORICAL_TABLE)


def quote(val):
    return '\'%s\'' % str(val)


# delta should be like timedelta(hours = 1)
# start and end should be in datetime.strptime(s, "%Y-%m-%d  %H:%M:%S") format
def span(start, end, delta):
    start_date = str_to_datetime(start, DATE_FORMAT)
    end_date = str_to_datetime(end, DATE_FORMAT)
    cur_date = start_date
    while cur_date < end_date:
        yield cur_date
        cur_date += delta


def k_nearest(start, end):
    col_list = []
    change_step = get_change_step(EARLIEST, LATEST)
    data_need_to_predict = []
    # TODO: build new json metadata structure for prediction database
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

        earliest_year = my_predict_table.get_min_in_col(HISTORICAL_TABLE, 'YEAR')
        latest_year = my_predict_table.get_max_in_col(HISTORICAL_TABLE, 'YEAR')
        coefficient_year = get_coefficient(earliest_year, latest_year)

        earliest_month = my_predict_table.get_min_in_col(HISTORICAL_TABLE, 'MONTH')
        latest_month = my_predict_table.get_max_in_col(HISTORICAL_TABLE, 'MONTH')
        coefficient_month = get_coefficient(earliest_month, latest_month)

        earliest_day = my_predict_table.get_min_in_col(HISTORICAL_TABLE, 'DAY')
        latest_day = my_predict_table.get_max_in_col(HISTORICAL_TABLE, 'DAY')
        coefficient_day = get_coefficient(earliest_day, latest_day)

        earliest_week = my_predict_table.get_min_in_col(HISTORICAL_TABLE, 'WEEK')
        latest_week = my_predict_table.get_max_in_col(HISTORICAL_TABLE, 'WEEK')
        coefficient_week = get_coefficient(earliest_week, latest_week)

        earliest_hour = my_predict_table.get_min_in_col(HISTORICAL_TABLE, 'HOUR')
        latest_hour = my_predict_table.get_max_in_col(HISTORICAL_TABLE, 'HOUR')
        coefficient_hour = get_coefficient(earliest_hour, latest_hour)

        for every_hour in span(start, end, timedelta(hours=1)):
            time_tuple = every_hour.timetuple()
            year = quote(time_tuple[0])
            month = quote(time_tuple[1])
            day = quote(time_tuple[2])
            week = quote(time_tuple[6])
            hour = quote(time_tuple[3])
            distributed_year = distribute_attr(year, coefficient_year)
            distributed_month = distribute_attr(month, coefficient_month)
            distributed_day = distribute_attr(day, coefficient_day)
            distributed_week = distribute_attr(week, coefficient_week)
            distributed_hour = distribute_attr(hour, coefficient_hour)
            distributed_list = [distributed_year, distributed_month, distributed_day, distributed_week, distributed_hour]
            data_need_to_predict = [distributed_year, distributed_month, distributed_day, distributed_week,
                                    distributed_hour]

            my_predict_table.insert(PREDICTION_TABLE, col_list[:-1],
                                    [year, month, day, week, hour, distributed_year, distributed_month, distributed_day,# get 10 nearest data sample
                                     distributed_week, distributed_hour])
            nearest_data_set = get_nearest_in_10_data_samples(data_need_to_predict, change_step)
            # check validity, renew nearest_data_set
            nearest_data_set = eliminate_un_valid_data(nearest_data_set, distributed_list)

            # predict count
            neighbours_distance = get_neighbours_distance(nearest_data_set, data_need_to_predict)
            weighted_neighbours_distance = get_weighted_neighbours_distance(neighbours_distance)
            predict_count = get_weighted_estimated_car_num(neighbours_distance, weighted_neighbours_distance)
            my_predict_table.insert(PREDICTION_TABLE, 'PREDICT_CNT', predict_count)

        my_predict_table.commit_record()
        my_db.add_index(PREDICTION_TABLE, '', 'query_index', col_list[:-1])
        my_db.close()


def get_distance(date1_list, date2_list):
    distance = math.sqrt(
            math.pow(date1_list[0] - date2_list[0], 2) + math.pow((date1_list[1] - date2_list[1]), 2) + math.pow(
                    date1_list[2] - date2_list[2], 2) + math.pow(date1_list[3] - date2_list[3], 2) + math.pow(
                    date1_list[4] - date2_list[4], 2))
    return distance


# distribute all attrs from 0 to 100, calculate coefficient a and b in 0 = attr1a + b, 100 = attr2 + b (attr1 < attr2)
# return an array of integer, values are coefficients
def get_coefficient(attr1, attr2):
    coefficient = []
    a = math.floor(100 / (attr2 - attr1))
    b = math.floor(attr1 * a * (-1))
    coefficient.append(a)
    coefficient.append(b)
    return coefficient


def distribute_attr(attr, coefficient):
    distributed_attr = coefficient[0] * attr + coefficient[1]
    return distributed_attr


# save change ranges for different attrs in change_range
def get_change_step(earliest, latest):
    change_step = []
    for i in range(len(earliest)):
        change_step.append(math.floor(100 / (latest[i] - earliest[i])))
    return change_step


# data_need_to_predict includes attrs (year, month, day, week, hour)
# attrs in data_need_to_predict is distributed
def get_nearest_in_10_data_samples(data_need_to_predict, change_step):
    nearest_distance = sys.maxint
    nearest_data_set = [[]]
    cnt = 0
    sign = -1
    tmp_data_list = []
    ptr = 0
    mark = 0
    mark_cnt = 1
    convert_sign = 0
    # test_rst = []
    change_range_number = 1
    # 1 represents odd, -1 represents even
    traverse_time = 1
    while cnt < 10:
        if traverse_time == -1:
            sign *= -1
        for attr in data_need_to_predict:
            if mark < len(change_step):
                if ptr == mark and (
                                    attr + sign * change_step[mark] * change_range_number < 0 or attr + sign *
                            change_step[
                                mark] * change_range_number > 100):
                    change_range_number *= 2
                    sign *= -1
                    convert_sign += 1
                if ptr == mark:
                    tmp_data_list.append(attr + sign * change_range_number * change_step[mark])
                else:
                    tmp_data_list.append(attr)
            ptr += 1
            change_range_number = 1
        nearest_data_set.append(tmp_data_list)
        # comment part is getting the nearest most data which satisfy nearest_distance
        # distance = get_distance(tmp_data_list, data_need_to_predict)
        # if distance < nearest_distance:
        #     nearest_distance = distance
        #     nearest_data_set = tmp_data_list
        if convert_sign == 1:
            sign *= -1
        cnt += 1
        if mark_cnt == 2:
            mark += 1
        traverse_time *= -1
        ptr = 0
        tmp_data_list = []
        convert_sign = 0
        if mark_cnt == 2:
            mark_cnt = 1
        else:
            mark_cnt += 1
    return nearest_data_set


def data_is_valid(dis_attr_values, col_list):
    if my_history_table.check_valid_data(HISTORICAL_TABLE, col_list, dis_attr_values):
        return True
    else:
        return False


def eliminate_un_valid_data(dis_attrs_set, col_list):
    for dis_attr_values in dis_attrs_set:
        if not data_is_valid(dis_attr_values, col_list):
            dis_attrs_set.remove(dis_attr_values)
    return dis_attrs_set


def get_neighbours_distance(nearest_data_set, data_need_to_predict):
    neighbours_distance = []
    for data in nearest_data_set:
        neighbours_distance.append(get_distance(data, data_need_to_predict))
    return neighbours_distance


# get weighted distances using Gaussian Function
def get_weighted_neighbours_distance(neighbours_distance, sigma=10.0):
    weighted_neighbours_distance = []
    for distance in neighbours_distance:
        weighted_neighbours_distance.append(math.e ** (-distance ** 2 / (2 * sigma ** 2)))
    return weighted_neighbours_distance


# get prediction car number
def get_weighted_estimated_car_num(neighbours_distance, weighted_neighbours_distance):
    avg = 0
    total_weight = 0
    for distance in neighbours_distance:
        avg *= distance
    for weighted_distance in weighted_neighbours_distance:
        total_weight += weighted_distance
    return avg / total_weight



