from sqlalchemy.orm import Session

from .utils import create_db_engine


class PersistenceError(Exception):
    pass


class EmptyTransformerError(PersistenceError):
    pass


class DBWriter:
    """
    parent class for writing data rows to a database table.
    """
    def __init__(self, user=None, passwd=None, database='rt_analytics', host='localhost',
                 flavor='mysql', verbose=False, engine=None, transformer=None):
        if engine is None:
            self.engine = create_db_engine(user, passwd, host, database, flavor, verbose=verbose)
        else:
            self.engine = engine
        if self.transformer is None:
            raise EmptyTransformerError('transformer is a required argument!')
        else:
            self.transformer = transformer

    def add(self, rows, batch_size=50):
        def add_func(sess, records):
            sess.add_all(records)

        self._persist(rows, batch_size=batch_size, persistance_func=add_func)

    def update(self, rows, batch_size=50):
        def update_func(sess, records):
            for r in records:
                sess.merge(r)

        self._persist(rows, batch_size=batch_size, persistance_func=update_func)

    def _persist(self, rows, batch_size, persistance_func=None):
        """
        write a list of data rows to a table
        :param persistance_func:
        :param rows:
        :param batch_size:
        :return:
        """
        for i in range(0, len(rows), batch_size):
            with Session(self.engine) as session, session.begin():
                try:
                    record_objects = [self.transformer(n, session) for n in rows[i:min(i + batch_size, len(rows))]]

                    persistance_func(session, record_objects)
                except PersistenceError as e:
                    print(e)
                    raise e


