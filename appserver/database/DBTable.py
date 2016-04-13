import MySQLdb

import database


class DBTable:
    def __init__(self):
        try:
            self.connection = MySQLdb.connect(host=database.host,
                                              user=database.user,
                                              passwd=database.passwd,
                                              db=database.db,
                                              port=database.port)
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

    def create_table(self, table):
        try:
            cursor = self.connection.cursor()
            build_table = "CREATE TABLE %s" % table
            cursor.execute(build_table)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def remove_table(self, table):
        try:
            cursor = self.connection.cursor()
            remove_table = "DROP TABLE IF EXISTS %s" % table
            cursor.execute(remove_table)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def insert_col(self, table, col, type):
        try:
            cursor = self.connection.cursor()
            insert_col = "ALTER TABLE %s ADD %s %s" %table %col %type
            cursor.execute(insert_col)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def remove_col(self, table, col):
        try:
            cursor = self.connection.cursor()
            remove_col = "ALTER TABLE %s DROP %s %s" %table %col
            cursor.execute(remove_col)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # TODO: check index type
    def add_index(self, table, type, index_name, col_list):
        try:
            cursor = self.connection.cursor()
            add_index = "ALTER TABLE %s ADD  %s INDEX %s (%s)" %table %type %index_name %col_list
            cursor.execute(add_index)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def drop_index(self, table, index):
        try:
            cursor = self.connection.cursor()
            drop_index = "DROP INDEX %s ON %s " %table %index
            cursor.execute(drop_index)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def close(self):
        self.connection.close()
