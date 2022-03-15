import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.sql import text


class DatabaseInterface:
    def __init__(self, user=None, passwd=None, server='localhost', database='rt_analytics'):
        user = urllib.parse.quote(user)
        passwd = urllib.parse.quote(passwd)
        self.engine = create_engine(f'mysql+pymysql://{user}:{passwd}@{server}/{database}?charset=utf8mb4',
                                    echo=True)

    def read_df(self, table_name=None, cols=None, sql=None):
        with self.engine.connect() as conn:
            if sql is not None:
                return pd.read_sql(sql, con=conn)
            elif table_name is not None:
                if cols is None:
                    cols = '*'
                else:
                    cols = ', '.join(cols)
                return pd.read_sql(f'select {cols} from {table_name}', con=conn)

    def record_count(self, table_name):
        return self.execute(f'select count(*) from {table_name}')

    def execute(self, sql):
        with self.engine.connect() as conn:
            return conn.execute(text(sql))

    def execute_sql_file(self, sql_path):
        with open(sql_path) as f:
            self.execute(f.read().strip())

    def append_df(self, dataframe, table_name):
        self._write_df(dataframe, table_name, if_exists='append')

    def replace_df(self, dataframe, table_name):
        self.execute(f'TRUNCATE TABLE {table_name}')
        self._write_df(dataframe, table_name, if_exists='append')

    def _write_df(self, dataframe, table_name, if_exists=None):
        with self.engine.connect() as conn:
            dataframe.to_sql(table_name, conn, if_exists=if_exists, index=False)
