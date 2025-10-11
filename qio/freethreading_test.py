"""Ensure that qio is freethreading capable.

Applications can still import things that disable freethreading,
but qio should not cause freethreading to be disabled.
"""

import importlib
import pkgutil
import sys

import pytest


@pytest.mark.skipif(
    not hasattr(sys, "_is_gil_enabled"),
    reason="Free-threading not supported in this Python build",
)
def test_all_qio_modules_preserve_freethreading():
    """Import all qio modules and ensure GIL remains disabled."""
    import qio

    for _, modname, _ in pkgutil.walk_packages(qio.__path__, f"{qio.__name__}."):
        if not modname.endswith("_test"):
            importlib.import_module(modname)

    assert sys._is_gil_enabled() is False
