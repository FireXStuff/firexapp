"""CI-oriented FireX tasks distributed by the owning firexapp package."""

from . import _version

__version__ = _version.get_versions()["version"]

__all__ = ["__version__"]
