from sqlalchemy.orm import Session

from .utils import create_db_engine


class DBWriter:
    def __init__(self, user=None, passwd=None, db='rt_analytics', host='localhost', flavor='mysql', verbose=False):
        user = urllib.parse.quote(user)
        passwd = urllib.parse.quote(passwd)

        self.engine = create_db_engine(user, passwd, host, db, flavor, verbose)

    def persist(self, rows, batch_size=50):
        for i in range(0, len(rows), batch_size):
            with Session(self.engine) as session, session.begin():
                try:
                    record_objects = [self.transform(n, session) for n in rows[i:min(i + batch_size, len(rows))]]
                    session.add_all(record_objects)
                except Exception as e:
                    print(e)
                    raise e

    def transform(self, row, session):
        pass

