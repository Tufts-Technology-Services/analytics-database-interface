from contextlib import nullcontext

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

    def transaction(self):
        """
        a connection whose work commits together; if anything inside raises, all of it is rolled
        back. Pass it as ``connection`` to the methods that take one:

            with db.transaction() as conn:
                db.execute("UPDATE fis_users SET fis_current = 'N'", connection=conn)
                db.upsert_df(fis_df, 'fis_users', connection=conn)
        """
        return self.engine.begin()

    def _in_transaction(self, connection=None):
        # the caller's transaction if given, otherwise a new one that commits on success
        return nullcontext(connection) if connection is not None else self.engine.begin()

    def read_df(self, table_name=None, cols=None, sql=None, connection=None):
        """
        read the contents of a table into a dataframe
        :param table_name:
        :param cols:
        :param sql:
        :param connection: optional, from transaction()
        :return:
        """
        with self._in_transaction(connection) as conn:
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

    def drop_table(self, table_name, connection=None):
        """
        drop the given table
        :param table_name:
        :param connection: optional, from transaction()
        :return:
        """
        self.execute(f'DROP TABLE IF EXISTS {table_name}', connection=connection)

    def execute(self, sql, format_strings=None, connection=None):
        """
        execute a SQL command and commit it (or run it in ``connection``, from transaction()).
        Rows from a query are read before returning, so the result works after the connection
        has gone back to the pool.
        :param sql:
        :param format_strings: bind parameters
        :param connection: optional, from transaction()
        :return:
        sqlstr = 'DELETE FROM storage_starfish_path where _id in ' + id_to_update
        # result = db_execute(sqlstr, con)
        db.execute(sqlstr)
        """
        # begin() commits on success; a plain connect() is rolled back on close under SQLAlchemy 2
        with self._in_transaction(connection) as conn:
            result = conn.execute(text(sql), format_strings or {})
            return result.freeze()() if result.returns_rows else result

    def execute_batch(self, sql_batch: list, connection=None):
        """
        execute a batch of SQL commands in a single transaction
        :param sql_batch:
        :param connection: optional, from transaction()
        :return:
        """
        with self._in_transaction(connection) as conn:
            for sql in sql_batch:
                conn.execute(text(sql))

    def execute_sql_file(self, sql_path, format_strings=None, connection=None):
        """
        execute a SQL command from a text file
        :param sql_path:
        :param format_strings:
        :param connection: optional, from transaction()
        :return:
        """
        with open(sql_path) as f:
            return self.execute(f.read().replace('\n', ' ').strip(), format_strings, connection=connection)

    def has_table(self, table_name):
        """
        checks whether a given table exists in the database
        :param table_name:
        :return:
        """
        insp = inspect(self.engine)
        return insp.has_table(table_name)

    def append_df(self, dataframe, table_name, connection=None):
        """
        append the contents of a dataframe to the given table
        :param dataframe:
        :param table_name:
        :param connection: optional, from transaction()
        :return:
        """
        self._write_df(dataframe, table_name, if_exists='append', connection=connection)

    def upsert_df(self, dataframe, table_name, temp_table='temp_table', schema=None, match_columns=None,
                  insert_only=False, connection=None):
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
        # modified: SQLite and MySQL/MariaDB support, dialect quoting, DO NOTHING when there's
        # nothing to update, temp table dropped afterwards, optional caller transaction

        Perform an "upsert" on a PostgreSQL, SQLite or MySQL/MariaDB table from a DataFrame.
        Uploads the DataFrame to a temporary table, then inserts from it: INSERT … ON CONFLICT
        on PostgreSQL and SQLite, INSERT … ON DUPLICATE KEY UPDATE on MySQL/MariaDB.
        Parameters
        ----------
        dataframe : pandas.DataFrame
            The DataFrame to be upserted.
        table_name : str
            The name of the target table.
        temp_table : str, optional
            The name of the temporary table the DataFrame is uploaded to.
        schema : str, optional
            The name of the schema containing the target table.
        match_columns : list of str, optional
            A list of the column name(s) on which to match. If omitted, the
            primary key columns of the target table will be used. MySQL ignores this and
            matches on any primary key or unique index of the table.
        insert_only : bool, optional
            On conflict do not update. (Default: False)
        connection : optional
            A connection from transaction(), to upsert as part of a larger transaction.
        """
        if not match_columns:
            insp = inspect(self.engine)
            match_columns = insp.get_pk_constraint(table_name, schema=schema)["constrained_columns"]
        if not match_columns:
            raise ValueError(f'{table_name} has no primary key: pass match_columns')
        table_spec = self._table_spec(table_name, schema)
        temp_spec = self._temp_table_spec(temp_table)
        stmt = self._upsert_sql(table_name, temp_table, list(dataframe.columns), match_columns, insert_only, schema)
        # only ever drop the temporary table, never a permanent one with the same name
        drop_temp = f"{'DROP TEMPORARY TABLE' if self._is_mysql() else 'DROP TABLE'} IF EXISTS {temp_spec}"

        with self._in_transaction(connection) as conn:
            conn.exec_driver_sql(drop_temp)
            conn.exec_driver_sql(f"CREATE TEMPORARY TABLE {temp_spec} AS SELECT * FROM {table_spec} WHERE false")
            dataframe.to_sql(temp_table, conn, if_exists="append", index=False)
            conn.exec_driver_sql(stmt)
            # pooled connections keep temporary tables alive, so don't leave the copy behind
            conn.exec_driver_sql(drop_temp)

    def _quote(self, name):
        return self.engine.dialect.identifier_preparer.quote_identifier(name)

    def _is_mysql(self):
        return self.engine.dialect.name in ('mysql', 'mariadb')

    def _table_spec(self, table_name, schema=None):
        return f'{self._quote(schema)}.{self._quote(table_name)}' if schema else self._quote(table_name)

    def _temp_table_spec(self, temp_table):
        # qualified with the temporary schema where the database has one
        temp_schema = {'postgresql': 'pg_temp.', 'sqlite': 'temp.'}.get(self.engine.dialect.name, '')
        return temp_schema + self._quote(temp_table)

    def _upsert_sql(self, table_name, temp_table, df_columns, match_columns, insert_only, schema=None):
        """
        the INSERT statement upsert_df runs, for this engine's dialect
        """
        quote = self._quote
        table_spec = self._table_spec(table_name, schema)
        temp_spec = self._temp_table_spec(temp_table)
        columns_to_update = [col for col in df_columns if col not in match_columns]
        insert_col_list = ", ".join(quote(col) for col in df_columns)

        if self._is_mysql():
            # columns of the SELECT's table can be referenced in ON DUPLICATE KEY UPDATE
            stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
            stmt += f"SELECT {insert_col_list} FROM {temp_spec} AS new_rows\n"
            stmt += "ON DUPLICATE KEY UPDATE "
            if insert_only or not columns_to_update:
                # a no-op update (INSERT IGNORE would also hide unrelated errors); the column is
                # qualified because new_rows has one with the same name
                no_op = quote(match_columns[0])
                return stmt + f"{no_op} = {table_spec}.{no_op}"
            return stmt + ", ".join(f"{quote(col)} = new_rows.{quote(col)}" for col in columns_to_update)

        if self.engine.dialect.name not in ('postgresql', 'sqlite'):
            raise NotImplementedError(f'upsert_df does not support {self.engine.dialect.name}')
        stmt = f"INSERT INTO {table_spec} ({insert_col_list})\n"
        # SQLite needs a WHERE clause to tell ON CONFLICT from a join's ON; harmless on PostgreSQL
        stmt += f"SELECT {insert_col_list} FROM {temp_spec} WHERE true\n"
        match_col_list = ", ".join(quote(col) for col in match_columns)
        stmt += f"ON CONFLICT ({match_col_list}) DO "
        if insert_only or not columns_to_update:
            return stmt + "NOTHING"
        return stmt + "UPDATE SET\n" + ", ".join(
            f"{quote(col)} = EXCLUDED.{quote(col)}" for col in columns_to_update)

    def replace_df(self, dataframe, table_name, connection=None):
        """
        replace the contents of a table with the contents of a dataframe, in one transaction:
        if the insert fails, the table keeps its old rows
        :param dataframe:
        :param table_name:
        :param connection: optional, from transaction()
        :return:
        """
        self._check_not_empty(dataframe)
        with self._in_transaction(connection) as conn:
            # DELETE rather than TRUNCATE: readers keep seeing the old rows until this commits,
            # where TRUNCATE's exclusive lock would block them for the whole load; and MySQL's
            # TRUNCATE commits implicitly
            conn.execute(text(f'DELETE FROM {table_name}'))
            self._write_df(dataframe, table_name, if_exists='append', connection=conn)

    def _write_df(self, dataframe, table_name, if_exists=None, connection=None):
        """
        writes a dataframe to a table
        :param dataframe:
        :param table_name:
        :param if_exists:
        :param connection: optional, from transaction()
        :return:
        """
        self._check_not_empty(dataframe)
        with self._in_transaction(connection) as conn:
            dataframe.to_sql(table_name, conn, if_exists=if_exists, index=False)

    @staticmethod
    def _check_not_empty(dataframe):
        if len(dataframe) == 0:
            raise Exception('Empty dataframe!')
