from sqlalchemy import create_engine, select, MetaData, Table
import pandas as pd
import argparse
import sys
import os


def dump_table(table_name, out_csv):
    user = os.getenv('RT_DB_USERNAME')
    passwd = os.getenv('RT_DB_PASSWORD')
    server = os.getenv('RT_DB_SERVER', 'localhost')
    database = os.getenv('RT_DB', 'rt_analytics')
    for i in [user, passwd]:
        if i is None:
            raise Exception("environment variables 'RT_DB_USERNAME' and 'RT_DB_PASSWORD' must be set")
    engine = create_engine(f"mysql+pymysql://{user}:{passwd}@{server}/{database}?charset=utf8mb4", future=True)
    metadata = MetaData(bind=None)
    table = Table(
        table_name,
        metadata,
        autoload=True,
        autoload_with=engine
    )

    stmt = select(table.columns)

    with engine.connect() as conn:
        df = pd.read_sql(stmt, conn)
        df.to_csv(out_csv, index=False)


def main(table_name, csv_path):
    try:
        dump_table(table_name, csv_path)
        return 0
    except ValueError as ve:
        return 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Dump tables in RT Analytics to CSV')
    parser.add_argument('table', help='name of the SQL table')
    parser.add_argument('csv', help='path to the output CSV')
    args = parser.parse_args()

    sys.exit(main(args.table, args.csv))
