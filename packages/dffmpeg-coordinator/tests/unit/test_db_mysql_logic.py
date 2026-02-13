from datetime import datetime
from unittest.mock import patch

import pytest

from dffmpeg.coordinator.db.jobs.mysql import MySQLJobRepository


@pytest.fixture
def mysql_job_repo():
    with patch("dffmpeg.coordinator.db.jobs.load") as mock_load:
        mock_load.return_value = MySQLJobRepository
        return MySQLJobRepository(
            host="localhost",
            port=3306,
            user="test",
            password="test",
            database="testdb",
            tablename="jobs",
            engine="mysql",
        )


def test_mysql_job_repo_stale_running_clause(mysql_job_repo):
    ts = datetime(2023, 1, 1, 12, 0, 0)
    clause = mysql_job_repo._get_stale_running_clause(1.5, ts)
    assert str(clause) == "worker_last_seen < DATE_SUB(:ts, INTERVAL (heartbeat_interval * :factor) SECOND)"
    assert clause.compile().params["ts"] == ts
    assert clause.compile().params["factor"] == 1.5


def test_mysql_job_repo_stale_assigned_clause(mysql_job_repo):
    ts = datetime(2023, 1, 1, 12, 0, 0)
    clause = mysql_job_repo._get_stale_assigned_clause(30, ts)
    assert str(clause) == "last_update < DATE_SUB(:ts, INTERVAL :timeout SECOND)"
    assert clause.compile().params["ts"] == ts
    assert clause.compile().params["timeout"] == 30


def test_mysql_job_repo_stale_monitored_clause(mysql_job_repo):
    ts = datetime(2023, 1, 1, 12, 0, 0)
    clause = mysql_job_repo._get_stale_monitored_clause(1.5, ts)
    assert str(clause) == "client_last_seen < DATE_SUB(:ts, INTERVAL (heartbeat_interval * :factor) SECOND)"
    assert clause.compile().params["ts"] == ts
    assert clause.compile().params["factor"] == 1.5


def test_mysql_job_repo_stale_pending_clause(mysql_job_repo):
    ts = datetime(2023, 1, 1, 12, 0, 0)
    clause = mysql_job_repo._get_stale_pending_clause(5, 30, ts)
    # clause is an and_ object containing text clauses
    compiled = clause.compile()
    # SQLAlchemy might use 'ts' or 'ts_1' depending on dialect/version/reuse
    # In some versions, bindparams keep their names if unique.
    assert ts in compiled.params.values()
    assert 5 in compiled.params.values()
    assert 30 in compiled.params.values()


def test_mysql_db_ssl_config():
    from dffmpeg.coordinator.db.engines.mysql import MySQLDB

    with patch("aiomysql.connect") as mock_connect:
        db = MySQLDB(
            host="localhost",
            tablename="test",
            ssl_ca="/path/to/ca.pem",
            ssl_cert="/path/to/cert.pem",
            ssl_key="/path/to/key.pem",
            ssl_verify=False,
        )
        with patch("ssl.create_default_context") as mock_ssl_ctx:
            mock_ctx_obj = mock_ssl_ctx.return_value
            db._connect()

            mock_ssl_ctx.assert_called_once_with(cafile="/path/to/ca.pem")
            mock_ctx_obj.load_cert_chain.assert_called_once_with(
                certfile="/path/to/cert.pem", keyfile="/path/to/key.pem"
            )
            assert mock_ctx_obj.check_hostname is False
            assert mock_ctx_obj.verify_mode == 0  # ssl.CERT_NONE

            mock_connect.assert_called_once()
            _, kwargs = mock_connect.call_args
            assert kwargs["ssl"] == mock_ctx_obj


def test_mysql_db_serialize_params_dict():
    from dffmpeg.coordinator.db.engines.mysql import MySQLDB

    db = MySQLDB(host="localhost", tablename="test")
    params = {
        "id": 1,
        "name": "test",
        "data": {"key": "value"},
        "tags": ["a", "b"],
    }
    serialized = db._serialize_params(params)
    assert type(serialized) is dict
    assert serialized["id"] == 1
    assert serialized["name"] == "test"
    assert serialized["data"] == '{"key": "value"}'
    assert serialized["tags"] == '["a", "b"]'


def test_mysql_db_serialize_params_list():
    from dffmpeg.coordinator.db.engines.mysql import MySQLDB

    db = MySQLDB(host="localhost", tablename="test")
    params = [1, "test", {"key": "value"}, ["a", "b"]]
    serialized = db._serialize_params(params)
    assert type(serialized) is list
    assert serialized[0] == 1
    assert serialized[1] == "test"
    assert serialized[2] == '{"key": "value"}'
    assert serialized[3] == '["a", "b"]'


def test_mysql_db_ssl_simple_config():
    from dffmpeg.coordinator.db.engines.mysql import MySQLDB

    with patch("aiomysql.connect") as mock_connect:
        db = MySQLDB(
            host="localhost",
            tablename="test",
            use_ssl=True,
        )
        with patch("ssl.create_default_context") as mock_ssl_ctx:
            mock_ctx_obj = mock_ssl_ctx.return_value
            db._connect()

            mock_ssl_ctx.assert_called_once_with(cafile=None)
            mock_ctx_obj.load_cert_chain.assert_not_called()

            mock_connect.assert_called_once()
            _, kwargs = mock_connect.call_args
            assert kwargs["ssl"] == mock_ctx_obj
