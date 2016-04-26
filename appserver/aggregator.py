import datetime

import utils
import json
from database import DBTable
from database import DBRecord
DATA_TABLE = 'carOccupancy'


def number_of_overlap(entries_file, exits_file):
    def quote(val):
        return '\'%s\'' % str(val)

    def record_car_num(time_stamp, car_cnt):
        time_tuple = time_stamp.timetuple()
        year = quote(time_tuple[0])
        month = quote(time_tuple[1])
        week = quote(time_tuple[2] / 7)
        weekday = quote(time_tuple[6])
        hour = quote(time_tuple[3])
        my_table.insert(col_list, [year, month, week, weekday, hour, quote(car_cnt)])

    num_of_cars = 0
    with open(entries_file, 'r') as etry, open(exits_file, 'r') as ext:
        fmt = "%Y-%m-%d  %H:%M:%S"
        entry_line = etry.readline().strip()
        exit_line = ext.readline().strip()
        my_db = DBTable()
        my_table = DBRecord(DATA_TABLE)
        col_list = []
        with open(
                '/Users/James/Desktop/PP/Learning/BitTiger/ParkingPrediction/parkingPrediction/appserver/database/HistoryDBMetaSchema.json') as data_file:
            data = json.load(data_file)
            if my_db.is_valid_table(DATA_TABLE):
                my_db.remove_table(DATA_TABLE)
            my_db.create_table(DATA_TABLE)
            for col_schema in data['metaSchema']['cols']:
                col_name = data['colsData'][col_schema]['name']
                col_type = data['colsData'][col_schema]['type']
                my_db.insert_col(DATA_TABLE, col_name, col_type)
                col_list.append(col_name.encode('utf-8'))
            # find num of cars
            last_time_stamp = datetime.datetime(1970, 1, 1)
            one_hour_time_diff = datetime.timedelta(0, 0, 0, 0, 0, 1)
            while entry_line != '' and exit_line != '':
                num_of_cars = num_of_cars
                entry_time = utils.str_to_datetime(entry_line, fmt)
                exit_time = utils.str_to_datetime(exit_line, fmt)
                if exit_line == '' or entry_time < exit_time:
                    num_of_cars += 1
                    if entry_time - last_time_stamp >= one_hour_time_diff:
                        record_car_num(entry_time, num_of_cars)
                        last_time_stamp = entry_time
                    entry_line = etry.readline().strip()
                else:
                    num_of_cars -= 1
                    if exit_time - last_time_stamp >= one_hour_time_diff:
                        record_car_num(exit_time, num_of_cars)
                        last_time_stamp = exit_time
                    exit_line = ext.readline().strip()

            my_table.commit_record()
            my_db.add_index(DATA_TABLE, 'query_index', col_list[:-1])
            my_db.close()
