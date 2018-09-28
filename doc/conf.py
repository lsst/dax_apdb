"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.dax.ppdb


_g = globals()
_g.update(build_package_configs(
    project_name='dax_ppdb',
    version=lsst.dax.ppdb.version.__version__))
