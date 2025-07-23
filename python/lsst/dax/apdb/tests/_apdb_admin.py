# This file is part of dax_apdb.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (http://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

__all__ = ["ApdbAdminTest"]

from abc import ABC, abstractmethod
from typing import Any

from .. import ApdbConfig
from .utils import TestCaseMixin


class ApdbAdminTest(TestCaseMixin, ABC):
    """Base class for ApdbAdmin tests that can be specialized for concrete
    implementation.

    This can only be used as a mixin class for a unittest.TestCase and it
    calls various assert methods.

    There are no common test methods in this class yet, they may be added in
    the future.
    """

    @abstractmethod
    def make_instance(self, **kwargs: Any) -> ApdbConfig:
        """Make database instance and return configuration for it."""
        raise NotImplementedError()
