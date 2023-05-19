import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.sql import text
from .utils import create_db_engine
import datetime
import logging


log = logging.getLogger(__name__)


class DatabaseInterface:
    """
    Used to handle low level database operations and centralize database code.
    """
    def __init__(self, user=None, passwd=None, host='localhost',
                 database='rt_analytics', flavor='mysql', verbose=False, engine=None):
        """
        provide database connection string info. alternatively, pass an existing SQLAlchemy engine
        :param user:
        :param passwd:
        :param host:
        :param database:
        :param flavor:
        :param verbose:
        :param engine:
        """
        if engine is None:
            self.engine = create_db_engine(user, passwd, host, database, flavor, verbose=verbose)
        else:
            self.engine = engine
        self.database = database

    def read_df(self, table_name=None, cols=None, sql=None):
        """
        read the contents of a table into a dataframe
        :param table_name:
        :param cols:
        :param sql:
        :return:
        """
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
        """
        returns 0 if the table is empty
        :param table_name:
        :return:
        """
        r = self.execute(f'select coalesce(count(*), 0) from {table_name}')
        return r.fetchone()[0]

    def check_last_date(self, date_col, table_name, where_clause=None) -> datetime.date:
        """
        :param date_col:
        :param table_name:
        :param where_clause:
        :returnL None if no records
        """
        query = f"select max({date_col}) from {table_name}"
        if where_clause is not None:
            query = f"{query} {where_clause}"
        r = self.execute(query)
        latest = r.fetchone()[0]
        if latest is not None and type(latest) is str:
            try:
                return datetime.datetime.strptime(latest, '%Y-%m-%d')
            except ValueError as e:
                log.error(e)
                log.error(f'latest is [{latest}] of type [{type(latest)}]')
                raise e
        else:
            return latest

    def fetch(self, sql):
        """
        fetch all records for the given SQL
        :param sql:
        :return:
        """
        r = self.execute(sql)
        return r.fetchall()

    def execute(self, sql):
        """
        execute a SQL command
        :param sql:
        :return:
        """
        with self.engine.connect() as conn:
            return conn.execute(text(sql))

    def execute_batch(self, sql_batch: list):
        """
        execute a batch of SQL commands in a single transaction
        :param sql_batch:
        :return:
        """
        with self.engine.connect() as conn:
            for sql in sql_batch:
                conn.execute(text(sql))

    def execute_sql_file(self, sql_path, format_strings=None):
        """
        execute a SQL command from a text file
        :param sql_path:
        :param format_strings:
        :return:
        """
        if format_strings is None:
            format_strings = {}
        with open(sql_path) as f:
            return self.execute(f.read().strip().format(**format_strings))

    def has_table(self, table_name):
        """
        checks whether a given table exists in the database
        :param table_name:
        :return:
        """
        insp = inspect(self.engine)
        return insp.has_table(table_name, schema=self.database)

    def append_df(self, dataframe, table_name):
        """
        append the contents of a dataframe to the given table
        :param dataframe:
        :param table_name:
        :return:
        """
        self._write_df(dataframe, table_name, if_exists='append')

    def replace_df(self, dataframe, table_name):
        """
        replace the contents of a table with the contents of a dataframe
        :param dataframe:
        :param table_name:
        :return:
        """
        self.execute(f'TRUNCATE TABLE {table_name}')
        self._write_df(dataframe, table_name, if_exists='append')

    def _write_df(self, dataframe, table_name, if_exists=None):
        """
        writes a dataframe to a table
        :param dataframe:
        :param table_name:
        :param if_exists:
        :return:
        """
        if len(dataframe) == 0:
            raise Exception('Empty dataframe!')
        with self.engine.connect() as conn:
            dataframe.to_sql(table_name, conn, if_exists=if_exists, index=False)
