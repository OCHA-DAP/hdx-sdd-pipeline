"""Runtime version information for the SDD pipeline."""

from importlib.metadata import PackageNotFoundError, version

PACKAGE_NAME = 'hdx-ssd-pipeline'
FALLBACK_VERSION = '0.1.0'

try:
    __version__ = version(PACKAGE_NAME)
except PackageNotFoundError:
    __version__ = FALLBACK_VERSION
