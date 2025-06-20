import pytest


@pytest.fixture
def temp_db_file(tmp_path_factory):
    """A fixture that provides a temporary file path for the test database."""
    return tmp_path_factory.mktemp("data") / "test.db"
