import pytest
from ulid import ULID

from dffmpeg.coordinator.db.jobs import JobRecord
from dffmpeg.coordinator.db.jobs.sqlite import SQLiteJobRepository


@pytest.mark.anyio
async def test_migration_adds_column(tmp_path):
    """Test that the migration logic successfully alters an old schema to add missing columns."""
    db_path = tmp_path / "test_migration.db"

    # 1. We create the repository to get the connection and the `Table` definition.
    repo = SQLiteJobRepository(engine="sqlite", path=str(db_path))

    # 2. Call setup() to create the full modern table schema
    await repo.setup()

    # 3. Drop the column to simulate an older schema
    # SQLite 3.35.0+ supports ALTER TABLE DROP COLUMN.
    await repo.execute("ALTER TABLE jobs DROP COLUMN working_directory")

    # Verify working_directory is NOT in the database anymore
    existing_columns = await repo.get_existing_columns()
    assert "working_directory" not in existing_columns

    # 4. Call migrate()
    # It should detect that `working_directory` is in repo.table.columns but not existing_columns,
    # and execute the ALTER TABLE statement.
    await repo.migrate()

    # 5. Verify working_directory has been added
    new_existing_columns = await repo.get_existing_columns()
    assert "working_directory" in new_existing_columns

    # 6. Verify the column is usable by creating and fetching a job
    sample_job = JobRecord(
        job_id=ULID(),
        requester_id="client1",
        binary_name="ffmpeg",
        status="pending",
        working_directory="/test/path",
        transport="http_polling",
        transport_metadata={},
    )
    await repo.create_job(sample_job)
    retrieved = await repo.get_job(sample_job.job_id)
    assert retrieved.working_directory == "/test/path"
