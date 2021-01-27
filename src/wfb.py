import firebird.driver as fdb


class FirebirdConnector:
    def __init__(self, *args, **kwargs):
        self.connection = fdb.connect(*args, **kwargs)
        self.columns = []

    def get(self, query, params=None):
        """
        Return list of data with base SELECT statement
        """
        cursor = self.connection.cursor()
        cursor.execute(query, params)
        self.columns = [col[0].lower() for col in cursor.description]
        return [dict(zip(self.columns, row)) for row in cursor.fetchall()]
