import MySQLdb


def sqlselect():
    return None


def create_connection():
    try:
        connect = MySQLdb.connect(host='localhost', user='root', passwd='', db='parkingDetails', port=3306)
        cursor = connect.cursor()

        # TODO: build sqlselect
        cursor.execute(sqlselect)
        result = cursor.fetchall()
        cursor.close()
        connect.close()
    except MySQLdb.Error, e:
        print('MySQLdb error %s: %s' % (e.args[0], e.args[1]))
