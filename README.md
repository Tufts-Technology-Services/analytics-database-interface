# database-interface

Code for interfacing with the rt_analytics database (PostgreSQL; MySQL/MariaDB also supported).

## Installation

Install with the extra for your database's driver, `postgres` or `mysql`:

```
pip install "database-interface[postgres] @ git+https://github.com/Tufts-Technology-Services/analytics-database-interface.git"
```

Upgrading to 0.4.0: PyMySQL is no longer installed by default (use the `mysql` extra), and
SQLAlchemy 2 and pandas 2.1.2 or later are required.

## Usage

Create a new instance:

```python
db = DatabaseInterface(user=user, passwd=passwd)
```

to specify database and server values:
```python
db = DatabaseInterface(user=user, passwd=passwd, server='localhost', database='rt_analytics', flavor='postgres')  # defaults shown
```


Read a table into a Pandas dataframe:
```python
fis_df = db.read_df('fis_users')
```

Upsert records (PostgreSQL, SQLite, MySQL/MariaDB; matches on the primary key unless `match_columns` is given):
```python

db.upsert_df(dataframe=fis_df, table_name='fis_users')
```

Append a Pandas dataframe to a table (only with postgres):
```python
db.append_df(dataframe=fis_df, table_name='fis_users')

```

Replace a table with a Pandas dataframe (in one transaction, so a failed insert keeps the old rows):
```python
db.replace_df(dataframe=fis_df, table_name='fis_users')

```

Run several operations in one transaction (if any of them fails, none are committed):
```python
with db.transaction() as conn:
    db.execute("UPDATE fis_users SET fis_current = 'N'", connection=conn)
    db.upsert_df(dataframe=fis_df, table_name='fis_users', connection=conn)
```

Execute arbitrary sql:
```python
db.execute('truncate fis_users')
```

Execute sql from a file:
```python
db.execute_sql_file("./path_to_file/file.sql")
```

Return the number of records in a table:
```python
count = db.record_count('fis_users')
```

## Running tests

The tests use SQLite files, so they need no database server:

```
uv run pytest
```
