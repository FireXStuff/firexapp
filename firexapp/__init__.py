from importlib.metadata import version as _distribution_version


__version__ = _distribution_version("firexapp")

del _distribution_version
