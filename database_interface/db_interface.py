import pandas as pd
from sqlalchemy import inspect
from sqlalchemy.sql import text
from database_interface.utils import create_db_engine
import datetime
import logging


log = logging.getLogger(__name__)


class DatabaseInterface:
    """
    Used to handle low level database operations and centralize database code.
    """
    def __init__(self, user=None, passwd=None, host='localhost',
                 database='rt_analytics', verbose=False, engine=None, flavor='postgres'):
        """
        provide database connection string info. alternatively, pass an existing SQLAlchemy engine
        :param user:
        :param passwd:
        :param host:
        :param database:
        :param verbose:
        :param engine:
        :param flavor:
        """
        if engine is None:
            self.engine = create_db_engine(user, passwd, host, database, verbose=verbose, flavor=flavor)
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

    def drop_table(self, table_name):
        """
        drop the given table
        :param table_name:
        :return:
        """
        sql = f'DROP TABLE IF EXISTS {table_name}'
        with self.engine.connect() as conn:
            conn.execute(sql)
            

    def execute(self, sql, format_strings={}):
        """
        execute a SQL command
        :param sql:
        :return:
        sqlstr = 'DELETE FROM storage_starfish_path where _id in ' + id_to_update
        # result = db_execute(sqlstr, con)
        db.execute(sqlstr)
        """
        with self.engine.connect() as conn:
            return conn.execute(text(sql), format_strings)

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
            return self.execute(f.read().replace('\n', ' ').strip(), format_strings)

    def has_table(self, table_name):
        """
        checks whether a given table exists in the database
        :param table_name:
        :return:
        """
        insp = inspect(self.engine)
        return insp.has_table(table_name)

    def append_df(self, dataframe, table_name):
        """
        append the contents of a dataframe to the given table
        :param dataframe:
        :param table_name:
        :return:
        """
        self._write_df(dataframe, table_name, if_exists='append')

    def upsert_df(self, dataframe, table_name, temp_table='temp_table', schema=None, match_columns=None, insert_only=False):
        """
        # Copyright 2024 Gordon D. Thompson, gord@gordthompson.com
        #
        # Licensed under the Apache License, Version 2.0 (the "License");
        # you may not use this file except in compliance with the License.
        # You may obtain a copy of the License at
        #
        #     http://www.apache.org/licenses/LICENSE-2.0
        #
        # Unless required by applicable law or agreed to in writing, software
        # distributed under the License is distributed on an "AS IS" BASIS,
        # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
        # See the License for the specific language governing permissions and
        # limitations under the License.

        # version 1.3 - 2024-06-21
        # from https://gist.github.com/gordthompson/ae7a1528fde1c00c03fdbb5c53c8f90f

        Perform an "upsert" on a PostgreSQL table from a DataFrame.
        Constructs an INSERT … ON CONFLICT statement, uploads the DataFrame to a
        temporary table, and then executes the INSERT.
        Parameters
        ----------
        dataframe : pandas.DataFrame
            The DataFrame to be upserted.
        table_name : str
            The name of the target table.
        engine : sqlalchemy.engine.Engine
            The SQLAlchemy Engine to use.
        schema : str, optional
            The name of the schema containing the target table.
        match_columns : list of str, optional
            A list of the column name(s) on which to match. If omitted, the
            primary key columns of the target table will be used.
        insert_only : bool, optional
            On conflict do not update. (Default: False)
        """
        table_spec = ""
        if schema:
            table_spec += '"' + schema.replace('"', '""') + '".'
        table_spec += '"' + table_name.replace('"', '""') + '"'

        df_columns = list(dataframe.columns)
        if not match_columns:
            insp = inspect(self.engine)
            match_columns = insp.get_pk_constraint(table_name, schema=schema)[
                "constrained_columns"
            ]
        columns_to_update = [col for col in df_columns if col not in match_columns]
        insert_col_list = ", ".join([f'"{col_name}"' for col_name in df_columns])
        stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
        stmt += f"SELECT {insert_col_list} FROM {temp_table}\n"
        match_col_list = ", ".join([f'"{col}"' for col in match_columns])
        stmt += f"ON CONFLICT ({match_col_list}) DO "
        if insert_only:
            stmt += "NOTHING"
        else:
            stmt += "UPDATE SET\n"
            stmt += ", ".join(
                [f'"{col}" = EXCLUDED."{col}"' for col in columns_to_update]
            )

        with self.engine.begin() as conn:
            conn.exec_driver_sql(f"DROP TABLE IF EXISTS {temp_table}")
            conn.exec_driver_sql(
                f"CREATE TEMPORARY TABLE {temp_table} AS SELECT * FROM {table_spec} WHERE false"
            )
            dataframe.to_sql(temp_table, conn, if_exists="append", index=False)
            conn.exec_driver_sql(stmt)

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
