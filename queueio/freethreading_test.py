"""Ensure that queueio is freethreading capable.

Applications can still import things that disable freethreading,
but queueio should not cause freethreading to be disabled.
"""

import importlib
import pkgutil
import sys

import pytest


@pytest.mark.skipif(
    not hasattr(sys, "_is_gil_enabled"),
    reason="Free-threading not supported in this Python build",
)
def test_all_queueio_modules_preserve_freethreading():
    """Import all queueio modules and ensure GIL remains disabled."""
    import queueio

    for _, modname, _ in pkgutil.walk_packages(
        queueio.__path__, f"{queueio.__name__}."
    ):
        if not modname.endswith("_test"):
            importlib.import_module(modname)

    assert sys._is_gil_enabled() is False
