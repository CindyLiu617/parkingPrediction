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
        command = "INSERT INTO %s (%s) VALUES (%s)" %table %col_list %val_list
        try:
            cursor.execute(command)
            self.connection.commit()
        except:
            self.connection.rollback()

    # TODO: ask what is op and value
    def get_col_value(self, table, col,row, op, condition):
        cursor = self.connection.cursor()
        command = "SELECT %s FROM %s WHERE %s %s %s" %col %table %row %op %condition
        try:
            cursor.execute(command)
            col_value = []
            result_set = cursor.fetchall()
            for row in result_set:
                col_value.append("%s") %row[col]
            return result_set
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # TODO: ask what is op and value
    def update_data(self,table, col, row, op, value, condition):
        cursor = self.connection.cursor()
        command = "UPDATE %s SET %s = %s WHERE %s %s %s " %table %col %value %row %op %condition
        try:
            cursor.execute(command)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # TODO: ask delete
    def delete(self):
        cursor = self.connection.cursor()
        try:
            self.connection.commit()
        except:
            self.connection.rollback()

    def close(self):
        self.connection.close()
