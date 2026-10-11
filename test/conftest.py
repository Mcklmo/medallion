import pytest


@pytest.fixture(autouse=True)
def _no_log_files(monkeypatch):
    # main() opens a log file on the root logger, which would outlive the test
    monkeypatch.setenv("MEDALLION_WRITE_LOG_FILES", "false")
