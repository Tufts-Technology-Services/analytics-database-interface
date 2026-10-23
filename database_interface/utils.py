from sqlalchemy import create_engine
import urllib.parse

# flavor -> the extra in pyproject.toml that installs its driver
DRIVER_EXTRAS = {'postgres': 'postgres', 'mysql': 'mysql'}


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
        connection_string = f'postgresql+psycopg2://{user}:{passwd}@{server}/{database}'
    elif flavor == 'mysql':
        connection_string = f'mysql+pymysql://{user}:{passwd}@{server}/{database}?charset=utf8mb4'
    elif flavor == 'mssql':
        connection_string = f'mssql+pymssql://{user}:{passwd}@{server}/{database}?charset=utf8'
    else:
        raise Exception(f'unimplemented database type {flavor}')
    try:
        return create_engine(connection_string, echo=verbose)
    except ModuleNotFoundError as e:
        # the drivers are optional extras
        hint = f'install database-interface[{DRIVER_EXTRAS[flavor]}]' if flavor in DRIVER_EXTRAS else f'install {e.name}'
        raise ModuleNotFoundError(f'the {flavor} flavor needs the {e.name} driver: {hint}', name=e.name) from e
