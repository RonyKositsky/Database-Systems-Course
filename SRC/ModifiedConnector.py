import mysql.connector


class ModifiedConnector:
    """
    This class will hold our connector and execute all our desired commands.
    """
    def __init__(self):
        self._connector = mysql.connector.connect(
            host='127.0.0.1',
            port=3305,
            user='DbMysql03',
            password='DbMysql03',
            database='DbMysql03',
        )
        self._cursor = self._connector.cursor()

    def execute(self, cmd):
        self._cursor.execute(cmd)
        self._connector.commit()

    def execute_with_params(self, cmd, data):
        self._cursor.execute(cmd, data)
        self._connector.commit()

    def close(self):
        self._connector.close()

    def fetch_data(self):
        return self._cursor.fetchall()
