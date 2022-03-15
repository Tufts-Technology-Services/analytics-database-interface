# database-interface

Code for interfacing with the rt_analytics MySQL database.

## Installation

add the following to the pyproject.toml file:
```
[tool.poetry.dependencies]
# Get the latest revision on the branch named "main"
database-interface = { git = "https://gitlab.it.tufts.edu/rt-analytics/database-interface.git", branch = "main" }

```

```
# Get a revision by its commit hash
database-interface = { git = "https://gitlab.it.tufts.edu/rt-analytics/database-interface.git", rev = "38eb5d3b" }

```

```
# Get a revision by its tag
database-interface = { git = "https://gitlab.it.tufts.edu/rt-analytics/database-interface.git", tag = "v0.13.2" }
```

## Usage

Create a new instance:

```python
db = DatabaseInterface(user=user, passwd=passwd)
```

to specify database and server values:
```python
db = DatabaseInterface(user=user, passwd=passwd, server='localhost', database='rt_analytics')  # defaults shown
```


Read a table into a Pandas dataframe:
```python
fis_df = db.read_df('fis_users')
```

Append a Pandas dataframe to a table:
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





