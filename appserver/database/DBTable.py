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
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # make a database connection and return it
    def __enter__(self):
        return self.connection

    # make sure db connection gets closed
    def __exit__(self):
        self.connection.close()

    # create table with one column
    def create_table(self, table):
        try:
            cursor = self.connection.cursor()
            build_table = "CREATE TABLE %s (%s %s, PRIMARY KEY (%s))" % (table, 'pid', 'MEDIUMINT NOT NULL AUTO_INCREMENT', 'pid')
            cursor.execute(build_table)
            self.connection.commit()
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def remove_table(self, table):
        try:
            cursor = self.connection.cursor()
            remove_table = "DROP TABLE %s" % table
            cursor.execute(remove_table)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def insert_col(self, table, col, col_type):
        try:
            cursor = self.connection.cursor()
            insert_col = "ALTER TABLE %s ADD %s %s" % (table, col, col_type)
            cursor.execute(insert_col)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def remove_col(self, table, col):
        try:
            cursor = self.connection.cursor()
            remove_col = "ALTER TABLE %s DROP %s" % (table, col)
            cursor.execute(remove_col)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    # TODO: check index type
    def add_index(self, table, index_type, index_name, col_list):
        try:
            cursor = self.connection.cursor()
            add_index = "ALTER TABLE %s ADD %s INDEX %s (%s)" % (table, index_type, index_name, col_list)
            cursor.execute(add_index)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def drop_index(self, table, index):
        try:
            cursor = self.connection.cursor()
            drop_index = "DROP INDEX %s ON %s" % (index, table)
            cursor.execute(drop_index)
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def is_db_existing(self, db):
        try:
            cursor = self.connection.cursor()
            check_db_existence = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = \'%s\'" % db
            cursor.execute(check_db_existence)
            return True if cursor.fetchone() else False
        except MySQLdb.Error, e:
            print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))

    def is_valid_table(self, table):
        cursor = self.connection.cursor()
        stmt = "SHOW TABLES LIKE \'%s\'" % table
        cursor.execute(stmt)
        return True if cursor.fetchone() else False

    def commit_table(self):
        self.connection.commit()

    def close(self):
        self.connection.close()
