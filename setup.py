# Need fastentrypoints to monkey patch setuptools for faster console_scripts.
# noinspection PyUnresolvedReferences
import fastentrypoints
from setuptools import setup

import versioneer


# Project metadata and dependencies live in pyproject.toml. Keep this shim
# temporarily for Versioneer and callers that still invoke setup.py directly.
setup(
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
)
