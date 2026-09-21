from unittest.mock import MagicMock

import pytest

from etl.bronze.ingestion import RowIngestion, validate_table


class SQLText(str):
    def format(self, *args):
        return SQLText(str(self).format(*args))

    def join(self, values):
        return SQLText(str(self).join(values))


def ingestion_fixture():
    ingestion = RowIngestion.__new__(RowIngestion)
    ingestion.table = "yellow_taxi_trips"
    ingestion.stage_name = "ingest_test"
    ingestion.columns = ["ingestion_timestamp", "source_file", "trip_distance"]
    ingestion.sql = MagicMock()
    ingestion.sql.SQL.side_effect = SQLText
    ingestion.sql.Identifier.side_effect = lambda name: f'"{name}"'
    ingestion.connection = MagicMock()
    return ingestion


@pytest.mark.parametrize("table", ["bad.name", "trips;DROP TABLE bronze.trips", "Trips"])
def test_invalid_table_is_rejected(table):
    with pytest.raises(ValueError):
        validate_table(table)


def test_publish_inserts_only_new_hashes_without_deleting_existing_rows():
    ingestion = ingestion_fixture()
    cursor = ingestion.connection.cursor.return_value.__enter__.return_value
    cursor.rowcount = 3
    assert ingestion.publish() == 3
    query = str(cursor.execute.call_args_list[-1].args[0])
    assert "ON CONFLICT (source_row_hash) DO NOTHING" in query
    assert '"source_file"' in query
    assert '"source_row_hash"' not in query  # Target trigger supplies the hash.
    assert all("DELETE" not in str(call) for call in cursor.execute.call_args_list)
    assert ingestion.connection.autocommit is True


def test_publish_failure_uses_transaction_context_and_restores_autocommit():
    ingestion = ingestion_fixture()
    cursor = ingestion.connection.cursor.return_value.__enter__.return_value
    cursor.execute.side_effect = [None, RuntimeError("merge failed")]
    with pytest.raises(RuntimeError, match="merge failed"):
        ingestion.publish()
    assert ingestion.connection.__exit__.call_args.args[0] is RuntimeError
    assert ingestion.connection.autocommit is True


def test_stage_excludes_hash_and_allows_null_hash_until_target_insert():
    ingestion = ingestion_fixture()
    cursor = ingestion.connection.cursor.return_value.__enter__.return_value
    cursor.fetchall.return_value = [("source_file",), ("trip_distance",), ("source_row_hash",)]
    assert ingestion.create_stage() == ["trip_distance"]
    assert ingestion.columns == ["source_file", "trip_distance"]
    assert "source_row_hash DROP NOT NULL" in str(cursor.execute.call_args.args[0])


def test_stage_is_cleaned_up_after_failure():
    ingestion = ingestion_fixture()
    ingestion.__exit__(RuntimeError, RuntimeError("failed"), None)
    cursor = ingestion.connection.cursor.return_value.__enter__.return_value
    assert "DROP TABLE IF EXISTS" in str(cursor.execute.call_args.args[0])
    ingestion.connection.close.assert_called_once()
