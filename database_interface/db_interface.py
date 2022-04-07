import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, inspect
from sqlalchemy.sql import text


class DatabaseInterface:
    def __init__(self, user=None, passwd=None, server='localhost',
                 database='rt_analytics', flavor='mysql', verbose=False):
        user = urllib.parse.quote(user)
        passwd = urllib.parse.quote(passwd)
        if flavor == 'mysql':
            connection_string = f'mysql+pymysql://{user}:{passwd}@{server}/{database}?charset=utf8mb4'
        elif flavor == 'mssql':
            connection_string = f'mssql+pymssql://{user}:{passwd}@{server}/{database}?charset=utf8'
        else:
            raise Exception(f'unimplemented database type {flavor}')
        self.engine = create_engine(connection_string, echo=verbose)
        self.database = database

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
        r = self.execute(f'select count(*) from {table_name}')
        return r.fetchone()[0]

    def fetch(self, sql):
        r = self.execute(sql)
        return r.fetchall()

    def execute(self, sql):
        with self.engine.connect() as conn:
            return conn.execute(text(sql))

    def execute_sql_file(self, sql_path, format_strings=None):
        if format_strings is None:
            format_strings = {}
        with open(sql_path) as f:
            return self.execute(f.read().strip().format(**format_strings))

    def has_table(self, table_name):
        insp = inspect(self.engine)
        return insp.has_table(table_name, schema=self.database)

    def append_df(self, dataframe, table_name):
        self._write_df(dataframe, table_name, if_exists='append')

    def replace_df(self, dataframe, table_name):
        self.execute(f'TRUNCATE TABLE {table_name}')
        self._write_df(dataframe, table_name, if_exists='append')

    def _write_df(self, dataframe, table_name, if_exists=None):
        with self.engine.connect() as conn:
            dataframe.to_sql(table_name, conn, if_exists=if_exists, index=False)
