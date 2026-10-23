"""DatabaseInterface against a SQLite file.

Each test gets its own database file, and results are read back over a new connection, so a
change only shows up if it was committed.
"""
import datetime

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from database_interface import DatabaseInterface


@pytest.fixture
def dbi(tmp_path):
    engine = create_engine(f"sqlite:///{tmp_path / 'rt_analytics.db'}")
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE people (utln TEXT PRIMARY KEY, name TEXT, report_date TEXT)"))
        conn.execute(text("INSERT INTO people VALUES ('u1', 'Ada', '2026-10-01'), ('u2', 'Grace', '2026-10-02')"))
    return DatabaseInterface(engine=engine)


def committed_rows(dbi, table_name="people"):
    with dbi.engine.connect() as conn:
        return [tuple(row) for row in conn.execute(text(f"SELECT * FROM {table_name} ORDER BY 1"))]


def people_frame(*rows):
    return pd.DataFrame(rows, columns=["utln", "name", "report_date"])


def test_execute_commits(dbi):
    dbi.execute("UPDATE people SET name = 'Ada L.' WHERE utln = 'u1'")
    assert committed_rows(dbi)[0] == ("u1", "Ada L.", "2026-10-01")


def test_execute_passes_bind_parameters(dbi):
    dbi.execute("DELETE FROM people WHERE utln = :utln", {"utln": "u2"})
    assert committed_rows(dbi) == [("u1", "Ada", "2026-10-01")]


def test_execute_results_can_be_read_after_it_returns(dbi):
    assert dbi.record_count("people") == 2
    assert dbi.fetch("SELECT utln FROM people ORDER BY utln") == [("u1",), ("u2",)]
    assert dbi.check_last_date("report_date", "people") == datetime.datetime(2026, 10, 2)


def test_execute_batch_commits_every_statement(dbi):
    dbi.execute_batch(["DELETE FROM people WHERE utln = 'u1'",
                       "INSERT INTO people VALUES ('u3', 'Katherine', '2026-10-03')"])
    assert [r[0] for r in committed_rows(dbi)] == ["u2", "u3"]


def test_execute_batch_is_one_transaction(dbi):
    with pytest.raises(Exception):
        dbi.execute_batch(["DELETE FROM people WHERE utln = 'u1'", "INSERT INTO no_such_table VALUES (1)"])
    assert [r[0] for r in committed_rows(dbi)] == ["u1", "u2"]


def test_execute_sql_file_commits(dbi, tmp_path):
    sql_file = tmp_path / "create_extra.sql"
    sql_file.write_text("CREATE TABLE IF NOT EXISTS extra (\n  id INTEGER PRIMARY KEY\n)\n")

    dbi.execute_sql_file(sql_file)

    assert dbi.has_table("extra")


def test_drop_table(dbi):
    dbi.drop_table("people")
    assert not dbi.has_table("people")


def test_replace_df_replaces_the_rows(dbi):
    new_rows = people_frame(("u3", "Katherine", "2026-10-03"))

    dbi.replace_df(new_rows, "people")
    dbi.replace_df(new_rows, "people")  # a second run must not duplicate rows

    assert committed_rows(dbi) == [("u3", "Katherine", "2026-10-03")]


def test_replace_df_keeps_the_old_rows_when_the_insert_fails(dbi):
    duplicate_keys = people_frame(("u3", "Katherine", "2026-10-03"), ("u3", "Again", "2026-10-03"))

    with pytest.raises(Exception):
        dbi.replace_df(duplicate_keys, "people")

    assert [r[0] for r in committed_rows(dbi)] == ["u1", "u2"]


def test_replace_df_refuses_an_empty_frame_and_leaves_the_table_alone(dbi):
    with pytest.raises(Exception, match="Empty dataframe"):
        dbi.replace_df(people_frame(), "people")
    assert len(committed_rows(dbi)) == 2


def test_append_df_commits(dbi):
    dbi.append_df(people_frame(("u3", "Katherine", "2026-10-03")), "people")
    assert [r[0] for r in committed_rows(dbi)] == ["u1", "u2", "u3"]


def test_read_df(dbi):
    df = dbi.read_df("people", cols=["utln", "name"])
    assert df.to_dict("records") == [{"utln": "u1", "name": "Ada"}, {"utln": "u2", "name": "Grace"}]
    assert dbi.read_df(sql="SELECT count(*) AS n FROM people").n.tolist() == [2]


# upsert_df

def test_upsert_df_updates_matches_and_inserts_new_rows(dbi):
    dbi.upsert_df(people_frame(("u2", "Grace H.", "2026-10-05"), ("u3", "Katherine", "2026-10-03")), "people")

    assert committed_rows(dbi) == [("u1", "Ada", "2026-10-01"), ("u2", "Grace H.", "2026-10-05"),
                                   ("u3", "Katherine", "2026-10-03")]


def test_upsert_df_insert_only_keeps_existing_rows(dbi):
    dbi.upsert_df(people_frame(("u2", "Grace H.", "2026-10-05"), ("u3", "Katherine", "2026-10-03")), "people",
                  insert_only=True)

    assert committed_rows(dbi)[1:] == [("u2", "Grace", "2026-10-02"), ("u3", "Katherine", "2026-10-03")]


def test_upsert_df_can_run_twice_with_the_same_temp_table(dbi):
    dbi.upsert_df(people_frame(("u3", "Katherine", "2026-10-03")), "people", temp_table="people_upsert")
    dbi.upsert_df(people_frame(("u3", "Kat", "2026-10-04")), "people", temp_table="people_upsert")

    assert committed_rows(dbi)[-1] == ("u3", "Kat", "2026-10-04")


def test_upsert_df_with_only_key_columns_does_nothing_on_conflict(dbi):
    dbi.upsert_df(pd.DataFrame({"utln": ["u1", "u4"]}), "people")
    assert [r[0] for r in committed_rows(dbi)] == ["u1", "u2", "u4"]


def dialect_interface(dialect_name):
    """A DatabaseInterface whose engine only has a dialect, enough to build SQL without a driver."""
    from types import SimpleNamespace
    from sqlalchemy.dialects import mssql, mysql, postgresql, sqlite

    dialects = {"postgresql": postgresql, "sqlite": sqlite, "mysql": mysql, "mssql": mssql}
    return DatabaseInterface(engine=SimpleNamespace(dialect=dialects[dialect_name].dialect()))


def test_upsert_sql_for_postgresql():
    sql = dialect_interface("postgresql")._upsert_sql("people", "tmp", ["utln", "name"], ["utln"], False)
    assert sql == ('INSERT INTO "people" ("utln", "name")\n'
                   'SELECT "utln", "name" FROM pg_temp."tmp" WHERE true\n'
                   'ON CONFLICT ("utln") DO UPDATE SET\n'
                   '"name" = EXCLUDED."name"')


def test_upsert_sql_for_mysql():
    sql = dialect_interface("mysql")._upsert_sql("people", "tmp", ["utln", "name", "report_date"], ["utln"], False)
    assert sql == ("INSERT INTO `people` (`utln`, `name`, `report_date`)\n"
                   "SELECT `utln`, `name`, `report_date` FROM `tmp` AS new_rows\n"
                   "ON DUPLICATE KEY UPDATE `name` = new_rows.`name`, `report_date` = new_rows.`report_date`")


def test_upsert_sql_for_mysql_insert_only_is_a_qualified_no_op_update():
    sql = dialect_interface("mysql")._upsert_sql("people", "tmp", ["utln", "name"], ["utln"], True)
    assert sql.endswith("ON DUPLICATE KEY UPDATE `utln` = `people`.`utln`")


def test_upsert_sql_with_a_schema():
    sql = dialect_interface("postgresql")._upsert_sql("people", "tmp", ["utln"], ["utln"], True, schema="rt")
    assert sql.startswith('INSERT INTO "rt"."people" ("utln")')


def test_upsert_sql_refuses_other_databases():
    with pytest.raises(NotImplementedError, match="mssql"):
        dialect_interface("mssql")._upsert_sql("people", "tmp", ["utln"], ["utln"], False)


def test_upsert_df_leaves_a_permanent_table_with_the_temp_name_alone(dbi):
    dbi.execute("CREATE TABLE temp_table (keep_me TEXT)")
    dbi.execute("INSERT INTO temp_table VALUES ('still here')")

    dbi.upsert_df(people_frame(("u3", "Katherine", "2026-10-03")), "people")

    assert committed_rows(dbi, "temp_table") == [("still here",)]
    assert len(committed_rows(dbi)) == 3


def test_upsert_df_drops_its_temp_table(dbi):
    with dbi.transaction() as conn:
        dbi.upsert_df(people_frame(("u3", "Katherine", "2026-10-03")), "people", connection=conn)
        temp_tables = conn.execute(text("SELECT name FROM sqlite_temp_master WHERE type = 'table'")).all()
    assert temp_tables == []


def test_upsert_df_needs_a_key_to_match_on(dbi):
    dbi.execute("CREATE TABLE no_key (utln TEXT, name TEXT)")
    with pytest.raises(ValueError, match="no_key has no primary key"):
        dbi.upsert_df(pd.DataFrame({"utln": ["u1"], "name": ["Ada"]}), "no_key")


def test_upsert_df_with_match_columns_on_a_table_without_a_primary_key(dbi):
    dbi.execute("CREATE TABLE keyed_by_index (utln TEXT, name TEXT)")
    dbi.execute("CREATE UNIQUE INDEX keyed_by_index_utln ON keyed_by_index (utln)")
    dbi.append_df(pd.DataFrame({"utln": ["u1"], "name": ["Ada"]}), "keyed_by_index")

    dbi.upsert_df(pd.DataFrame({"utln": ["u1"], "name": ["Ada L."]}), "keyed_by_index", match_columns=["utln"])

    assert committed_rows(dbi, "keyed_by_index") == [("u1", "Ada L.")]


# results and transactions

def test_execute_results_survive_the_connection_closing(dbi):
    result = dbi.execute("SELECT utln FROM people ORDER BY utln")
    dbi.engine.dispose()  # closes every pooled connection
    assert result.fetchall() == [("u1",), ("u2",)]


def test_execute_reports_rows_changed(dbi):
    assert dbi.execute("UPDATE people SET name = 'x'").rowcount == 2


def test_transaction_commits_everything_together(dbi):
    with dbi.transaction() as conn:
        dbi.execute("UPDATE people SET name = 'not current'", connection=conn)
        dbi.upsert_df(people_frame(("u1", "Ada", "2026-10-09")), "people", connection=conn)

    assert committed_rows(dbi) == [("u1", "Ada", "2026-10-09"), ("u2", "not current", "2026-10-02")]


def test_transaction_rolls_everything_back_on_failure(dbi):
    with pytest.raises(Exception):
        with dbi.transaction() as conn:
            dbi.execute("UPDATE people SET name = 'not current'", connection=conn)
            dbi.upsert_df(pd.DataFrame({"utln": ["u1"], "no_such_column": ["x"]}), "people", connection=conn)

    assert [r[1] for r in committed_rows(dbi)] == ["Ada", "Grace"]


def test_replace_df_inside_a_transaction_is_rolled_back_with_it(dbi):
    with pytest.raises(RuntimeError):
        with dbi.transaction() as conn:
            dbi.replace_df(people_frame(("u3", "Katherine", "2026-10-03")), "people", connection=conn)
            assert [r[0] for r in conn.execute(text("SELECT utln FROM people"))] == ["u3"]
            raise RuntimeError("abandon the transaction")

    assert [r[0] for r in committed_rows(dbi)] == ["u1", "u2"]


# missing drivers

@pytest.mark.parametrize(("flavor", "driver", "extra"), [("postgres", "psycopg2", "postgres"),
                                                         ("mysql", "pymysql", "mysql")])
def test_missing_driver_names_the_extra_to_install(flavor, driver, extra):
    import importlib.util

    from database_interface.utils import create_db_engine

    if importlib.util.find_spec(driver):
        pytest.skip(f"{driver} is installed")
    with pytest.raises(ModuleNotFoundError, match=rf"database-interface\[{extra}\]"):
        create_db_engine("EXAMPLE-user", "EXAMPLE-placeholder", flavor=flavor)
