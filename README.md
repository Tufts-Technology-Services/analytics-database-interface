# database-interface

Code for interfacing with the rt_analytics MySQL database.

## Installation

```
pip install https://github.com/Tufts-Technology-Services/analytics-database-interface.git

```

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

Upsert records
```python

db.upsert_df(dataframe=fis_df, table_name='fis_users')


Append a Pandas dataframe to a table (only with postgres):
```python
db.append_df(dataframe=fis_df, table_name='fis_users')

```

Replace a table with a Pandas dataframe:
```python
db.replace_df(dataframe=fis_df, table_name='fis_users')

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


```



