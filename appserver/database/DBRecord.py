import MySQLdb

import database


class DBRecord:
    def __init__(self, table):
        try:
            self.table = table
            self.connection = MySQLdb.connect(host=database.host,
                                              user=database.user,
                                              passwd=database.passwd,
                                              db=database.db,
                                              port=database.port)
            self.cursor = None
            # TODO: if db is not exist, create one
            # if :
            #     print('Connected to MySQL database')
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # make a database connection and return it
    def __enter__(self):
        return self.connection

    # make sure db connection gets closed
    def __exit__(self):
        self.connection.close()

    def insert(self, table, col_list, val_list):
        cursor = self.connection.cursor()
        command = "INSERT INTO %s (%s) VALUES (%s)" % (table, ','.join(col_list), ','.join(val_list))
        try:
            cursor.execute(command)
            # self.connection.commit()
        except:
            self.connection.rollback()

    def get_col_value(self, table, col_get, col_condition, op, condition):
        cursor = self.connection.cursor()
        command = "SELECT %s FROM %s WHERE %s %s %s" % (col_get, table, col_condition, op, condition)
        try:
            cursor.execute(command)
            col_value = []
            result_set = cursor.fetchall()
            for row in result_set:
                col_value.append("%s") % row[col_get]
            return result_set
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def update_data(self, table, col_get, col_condition, op, new_value, condition):
        cursor = self.connection.cursor()
        updata_data = "UPDATE %s SET %s = %s WHERE %s %s %s " % (
            table, col_get, new_value, col_condition, op, condition)
        try:
            cursor.execute(updata_data)
            self.connection.commit()
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def delete_data(self, table, col_get, col_condition, op, condition):
        cursor = self.connection.cursor()
        delete_data = "UPDATE %s SET %s = 'NULL' WHERE %s %s %s " % (table, col_get, col_condition, op, condition)
        try:
            cursor.execute(delete_data)
            self.connection.commit()
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def get_max_in_col(self, table, col):
        cursor = self.connection.cursor()
        get_max_in_col = "SELECT MAX(%s) AS max FROM %s" % (table, col)
        try:
            cursor.execute(get_max_in_col)
            max_val = cursor.fetchall()
            return max_val
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def get_min_in_col(self, table, col):
        cursor = self.connection.cursor()
        get_min_in_col = "SELECT MIN(%s) AS min FROM %s" % (table, col)
        try:
            cursor.execute(get_min_in_col)
            min_val = cursor.fetchall()
            return min_val
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # TODO: alter command line to iterable
    def check_valid_data(self, table, col_list, value_list):
        cursor = self.connection.cursor()
        command = "SELECT * FROM %s WHERE %s = %s and %s = %s and %s = %s and %s = %s and %s = %s" % (
            table, col_list[0], value_list[0], col_list[1], value_list[1], col_list[2], value_list[2], col_list[3],
            value_list[3], col_list[4], value_list[4])
        try:
            cursor.execute(command)
            result_set = cursor.fetchall()
            if result_set:
                return True
            else:
                return False
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def commit_record(self):
        self.connection.commit()

    def close(self):
        self.connection.close()
