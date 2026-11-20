"""Command line: exit codes and configuration errors reported without a traceback."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from pathlib import Path

import pytest

from mibel_ingestor.cli import EXIT_CONFIG_ERROR, EXIT_JOB_FAILED, main
from mibel_ingestor.connectors import get_connector_class
from mibel_ingestor.http import FetchError
from mibel_ingestor.model import Request


DEV_NULL_SINK = ["--mode", "dev", "--set", "sink.type=null"]


@pytest.fixture(autouse=True)
def restore_logging() -> Iterator[None]:
    """``main`` installs the node's logging on the root logger; put the previous handlers back."""
    root = logging.getLogger()
    previous_handlers = root.handlers[:]
    previous_level = root.level
    yield
    root.handlers[:] = previous_handlers
    root.setLevel(previous_level)


def fail_download(self: object, request: Request) -> bytes:
    """Stand-in for a download whose source is down."""
    raise FetchError("source down")


# ##################################################################################################
# ######################################     EXIT CODES     ########################################
# ##################################################################################################


def test_once_with_unknown_dataset_is_a_config_error(capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["once", *DEV_NULL_SINK, "--source", "omie", "--dataset", "day_ahaed"])
    assert exit_code == EXIT_CONFIG_ERROR
    assert "no enabled job matches" in capsys.readouterr().err


def test_missing_env_file_is_a_config_error(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    exit_code = main(["check", *DEV_NULL_SINK, "--env-file", str(tmp_path / "missing.env")])
    assert exit_code == EXIT_CONFIG_ERROR
    assert "env file not found" in capsys.readouterr().err


def test_backfill_with_failed_chunks_exits_with_job_failed(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setattr(get_connector_class("omie"), "download", fail_download)
    exit_code = main([
        "backfill",
        *DEV_NULL_SINK,
        "--source", "omie",
        "--dataset", "day_ahead",
        "--start", "2026-01-01",
        "--end", "2026-01-01",
    ])
    assert exit_code == EXIT_JOB_FAILED
    assert '"fetch_error"' in capsys.readouterr().out
