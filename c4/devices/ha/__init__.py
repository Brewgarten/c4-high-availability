"""
Copyright (c) IBM 2015-2017. All Rights Reserved.
Project name: c4-high-availability
This project is licensed under the MIT License, see LICENSE
"""
from pkgutil import extend_path

from ._version import get_versions


__path__ = extend_path(__path__, __name__)

__version__ = get_versions()['version']
del get_versions
