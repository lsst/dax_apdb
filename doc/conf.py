"""Sphinx configuration file for an LSST stack package.

This configuration only affects single-package Sphinx documenation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.dax.apdb


_g = globals()
_g.update(build_package_configs(
    project_name='dax_apdb',
    version=lsst.dax.apdb.version.__version__))
