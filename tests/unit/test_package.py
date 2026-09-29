from __future__ import annotations

import importlib.metadata

import pytest

import tallyho
from tallyho.cli import main
from tallyho.model import errors


def test_version_matches_metadata() -> None:
    assert tallyho.__version__ == importlib.metadata.version("tallyho")


@pytest.mark.parametrize(
    "exc_type",
    [errors.ConfigurationError, errors.NotFoundError, errors.InvalidStateError],
)
def test_errors_inherit_base(exc_type: type[Exception]) -> None:
    assert issubclass(exc_type, tallyho.TallyhoError)


def test_cli_without_args_returns_zero() -> None:
    assert main([]) == 0


def test_cli_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exc_info:
        main(["--version"])
    assert exc_info.value.code == 0
    assert tallyho.__version__ in capsys.readouterr().out
