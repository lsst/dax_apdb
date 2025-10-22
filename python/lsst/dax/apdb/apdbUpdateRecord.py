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

__all__ = [
    "ApdbCloseDiaObjectValidityRecord",
    "ApdbReassignDiaSourceRecord",
    "ApdbUpdateNDiaSourcesRecord",
    "ApdbUpdateRecord",
    "ApdbWithdrawDiaForcedSourceRecord",
    "ApdbWithdrawDiaSourceRecord",
]

import dataclasses
import json
from abc import ABC
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, ClassVar

from .apdb import ApdbTables


@dataclass(kw_only=True)
class ApdbUpdateRecord(ABC):
    """Abstract base class representing all types of update records saved to
    replica table.
    """

    update_time_ns: int
    """Time in nanoseconds since epoch when update happened."""

    update_order: int
    """Record order in the update."""

    update_type: ClassVar[str]
    """Class variable defining type of the update, must be defined in all
    concrete subclasses and be unique.
    """

    apdb_table: ClassVar[ApdbTables]
    """Class variable defining APDB table that this update applies to, must be
    defined in all concrete subclasses.
    """

    _update_types: ClassVar[dict[str, type[ApdbUpdateRecord]]] = {}
    """Class variable for mapping the type of the update to corresponding
    record class.
    """

    def __init_subclass__(cls, /, update_type: str, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)
        cls.update_type = update_type
        cls._update_types[update_type] = cls

    def __lt__(self, other: Any) -> bool:
        if isinstance(other, ApdbUpdateRecord):
            return (self.update_time_ns, self.update_order) < (other.update_time_ns, other.update_order)
        raise self._type_error(other)

    def __le__(self, other: Any) -> bool:
        if isinstance(other, ApdbUpdateRecord):
            return (self.update_time_ns, self.update_order) <= (other.update_time_ns, other.update_order)
        raise self._type_error(other)

    def __gt__(self, other: Any) -> bool:
        if isinstance(other, ApdbUpdateRecord):
            return (self.update_time_ns, self.update_order) > (other.update_time_ns, other.update_order)
        raise self._type_error(other)

    def __ge__(self, other: Any) -> bool:
        if isinstance(other, ApdbUpdateRecord):
            return (self.update_time_ns, self.update_order) >= (other.update_time_ns, other.update_order)
        raise self._type_error(other)

    def _type_error(self, other: Any) -> TypeError:
        return TypeError(
            "ordering is not supported between instances of "
            f"'{self.__class__.__name__}' and '{other.__class__.__name__}'"
        )

    @classmethod
    def from_json(cls, update_time_ns: int, update_order: int, json_str: str) -> ApdbUpdateRecord:
        json_obj = json.loads(json_str)
        if not isinstance(json_obj, Mapping):
            raise TypeError("String must contain JSON object.")
        kw = dict(json_obj)
        if (update_type := kw.pop("update_type", None)) is None:
            raise LookupError("`update_type` key is not in JSON object.")
        if (klass := cls._update_types.get(update_type)) is not None:
            return klass(update_time_ns=update_time_ns, update_order=update_order, **kw)
        else:
            raise ValueError(f"Unknown update type: {update_type}")

    def to_json(self) -> str:
        data = dataclasses.asdict(self)
        # These fields are stored separately.
        data.pop("update_time_ns")
        data.pop("update_order")
        data["update_type"] = self.update_type
        return json.dumps(data)


@dataclass(kw_only=True)
class ApdbReassignDiaSourceRecord(ApdbUpdateRecord, update_type="reassign_diasource"):
    """Update record representing re-assignment of DIASource to SSObject."""

    diaSourceId: int
    """ID of DIASource record."""

    diaObjectId: int
    """ID of associated DIAObject record."""

    ssObjectId: int
    """ID of SSObject to re-associate to."""

    ssObjectReassocTimeMjdTai: float
    """Time when DIASource was re-associated from DIAObject to SSObject."""

    ra: float
    """DIASource ra, not required to be exact, but needs to be close to the
    database record.
    """

    dec: float
    """DIASource dec, not required to be exact, but needs to be close to the
    database record.
    """

    apdb_table: ClassVar[ApdbTables] = ApdbTables.DiaSource


@dataclass(kw_only=True)
class ApdbWithdrawDiaSourceRecord(ApdbUpdateRecord, update_type="withdraw_diasource"):
    """Update record representing withdrawal of DIASource."""

    diaSourceId: int
    """ID of DIASource record."""

    diaObjectId: int | None
    """ID of associated DIAObject record or None if it is associated to
    SSObject.
    """

    timeWithdrawnMjdTai: float
    """Time when this record was marked invalid."""

    ra: float
    """DIASource ra, not required to be exact, but needs to be close to the
    database record.
    """

    dec: float
    """DIASource dec, not required to be exact, but needs to be close to the
    database record.
    """

    apdb_table: ClassVar[ApdbTables] = ApdbTables.DiaSource


@dataclass(kw_only=True)
class ApdbWithdrawDiaForcedSourceRecord(ApdbUpdateRecord, update_type="withdraw_diaforcedsource"):
    """Update record representing withdrawal of DIAForcedSource."""

    diaObjectId: int
    """ID of DIAObject for withdrawn DIAForcedSource."""

    visit: int
    """Visit ID of DIAForcedSource."""

    detector: int
    """Detector ID of DIAForcedSource."""

    timeWithdrawnMjdTai: float
    """Time when this record was marked invalid."""

    ra: float
    """DIAForcedSource ra, not required to be exact, but needs to be close to
    the database record.
    """

    dec: float
    """DIAForcedSource dec, not required to be exact, but needs to be close to
    the database record.
    """

    apdb_table: ClassVar[ApdbTables] = ApdbTables.DiaForcedSource


@dataclass(kw_only=True)
class ApdbCloseDiaObjectValidityRecord(ApdbUpdateRecord, update_type="close_diaobject_validity"):
    """Record representing closing of the validity interval of DIAObject."""

    diaObjectId: int
    """ID of DIAObject."""

    validityEndMjdTai: float
    """Time to set validityEnd to."""

    nDiaSources: int | None
    """New value for nDiaSources column for updated record, or None if
    nDiaSources does not change.
    """

    ra: float
    """DIAObject ra, not required to be exact, but needs to be close to the
    database record.
    """

    dec: float
    """DIAObject dec, not required to be exact, but needs to be close to the
    database record.
    """

    apdb_table: ClassVar[ApdbTables] = ApdbTables.DiaObject


@dataclass(kw_only=True)
class ApdbUpdateNDiaSourcesRecord(ApdbUpdateRecord, update_type="update_n_dia_sources"):
    """Record representing change in the number of associated sources of
    DIAObject.
    """

    diaObjectId: int
    """ID of DIAObject."""

    nDiaSources: int
    """New value for nDiaSources column for updated record."""

    ra: float
    """DIAObject ra, not required to be exact, but needs to be close to the
    database record.
    """

    dec: float
    """DIAObject dec, not required to be exact, but needs to be close to the
    database record.
    """

    apdb_table: ClassVar[ApdbTables] = ApdbTables.DiaObject
