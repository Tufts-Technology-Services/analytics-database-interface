from sqlalchemy.orm import Session

from database_interface.utils import create_db_engine


class PersistenceError(Exception):
    pass


class EmptyTransformerError(PersistenceError):
    pass


class DBWriter:
    """
     class for writing data rows to a database table.
    """
    def __init__(self, user=None, passwd=None, database='rt_analytics', host='localhost',
                 flavor='mysql', verbose=False, engine=None, transformer=None):
        if engine is None:
            self.engine = create_db_engine(user, passwd, host, database, flavor, verbose=verbose)
        else:
            self.engine = engine
        if transformer is None:
            raise EmptyTransformerError('transformer is a required argument!')
        else:
            self.transformer = transformer

    def add_or_update(self, row):
        """
        insert or update a data row to a table
        :param row:
        :return:
        """

        with Session(self.engine) as session, session.begin():
            try:
                row_obj, exists = self.transformer(row, session)
                if exists:
                    session.merge(row_obj)
                else:
                    session.add(row_obj)
            except PersistenceError as e:
                print(e)
                raise e


