from typing import Iterator

import pytest
from testcontainers.postgres import PostgresContainer  # type: ignore


@pytest.fixture
def temp_db_file(tmp_path_factory):
    """A fixture that provides a temporary file path for the test database."""
    return tmp_path_factory.mktemp("data") / "test.db"


@pytest.fixture
def postgres_container() -> Iterator[PostgresContainer]:
    with PostgresContainer("postgres:17-alpine").with_command(
        "-c max_connections=200"
    ) as postgres:
        yield postgres
