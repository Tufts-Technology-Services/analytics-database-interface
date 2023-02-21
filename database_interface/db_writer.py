from sqlalchemy.orm import Session

from .utils import create_db_engine


class DBWriter:
    """
    parent class for writing data rows to a database table.
    """
    def __init__(self, user=None, passwd=None, database='rt_analytics', host='localhost',
                 flavor='mysql', verbose=False, engine=None):
        if engine is None:
            self.engine = create_db_engine(user, passwd, host, database, flavor, verbose=verbose)
        else:
            self.engine = engine

    def persist(self, rows, batch_size=50):
        """
        write a list of data rows to a table
        :param rows:
        :param batch_size:
        :return:
        """
        for i in range(0, len(rows), batch_size):
            with Session(self.engine) as session, session.begin():
                try:
                    record_objects = [self.transform(n, session) for n in rows[i:min(i + batch_size, len(rows))]]
                    session.add_all(record_objects)
                except Exception as e:
                    print(e)
                    raise e

    def transform(self, row, session):
        """
        abstract function for transforming a data row from its native form to the format expected by the database.
        override this.
        :param row:
        :param session:
        :return:
        """
        pass

