from sqlalchemy import create_engine
import urllib.parse


def create_db_engine(user=None, passwd=None, server='localhost', database='rt_analytics',
                     flavor='postgres', verbose=False):
    """
    create supported SQLAlchemy database engine for RT Analytics
    :param user:
    :param passwd:
    :param server:
    :param database:
    :param flavor:
    :param verbose:
    :return:
    """
    user = urllib.parse.quote(user)
    passwd = urllib.parse.quote(passwd)
    if flavor == 'postgres':
        connection_string = f'postgresql+psycopg://{user}:{passwd}@{server}/{database}'
    elif flavor == 'mysql':
        connection_string = f'mysql+pymysql://{user}:{passwd}@{server}/{database}?charset=utf8mb4'
    elif flavor == 'mssql':
        connection_string = f'mssql+pymssql://{user}:{passwd}@{server}/{database}?charset=utf8'
    else:
        raise Exception(f'unimplemented database type {flavor}')
    return create_engine(connection_string, echo=verbose)
