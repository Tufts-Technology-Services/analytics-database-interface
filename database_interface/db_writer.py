from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import urllib.parse


class DBWriter:
    def __init__(self, user=None, passwd=None):
        user = urllib.parse.quote(user)
        passwd = urllib.parse.quote(passwd)
        self.engine = create_engine(f'mysql+pymysql://{user}:{passwd}@localhost/rt_analytics?charset=utf8mb4', echo=True)

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

