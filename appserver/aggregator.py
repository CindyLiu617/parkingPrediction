import utils
import json
from database import DBTable
from database import DBRecord
DATA_TABLE = 'carOccupancy'


def number_of_overlap(entries_file, exits_file):
    def quote(val):
        return '\'%s\'' % str(val)

    num_of_cars = 0
    with open(entries_file, 'r') as etry, open(exits_file, 'r') as ext:
        fmt = "%Y-%m-%d  %H:%M:%S"
        entry_line = etry.readline().strip()
        exit_line = ext.readline().strip()
        # database creation and cols insertion
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
            while entry_line != '' and exit_line != '':
                num_of_cars = num_of_cars
                entry_time = utils.str_to_datetime(entry_line, fmt)
                exit_time = utils.str_to_datetime(exit_line, fmt)
                if exit_line == '' or entry_time < exit_time:
                    num_of_cars += 1
                    entry_line = etry.readline().strip()
                else:
                    num_of_cars -= 1
                    exit_line = ext.readline().strip()
                # database values insertion
                time_tuple = entry_time.timetuple()
                year = quote(time_tuple[0])
                month = quote(time_tuple[1])
                day = quote(time_tuple[2])
                week = quote(time_tuple[6])
                hour = quote(time_tuple[3])
                minute = quote(time_tuple[4])
                my_table.insert(DATA_TABLE, col_list, [year, month, day, week, hour, minute, quote(num_of_cars)])

            my_table.commit_record()
            my_db.add_index(DATA_TABLE, '', 'query_index', col_list[:-1])
            my_db.close()
