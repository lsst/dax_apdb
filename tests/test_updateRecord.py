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

import json
import unittest

from lsst.dax.apdb.apdbUpdateRecord import (
    ApdbCloseDiaObjectValidityRecord,
    ApdbReassignDiaSourceRecord,
    ApdbUpdateNDiaSourcesRecord,
    ApdbUpdateRecord,
    ApdbWithdrawDiaForcedSourceRecord,
    ApdbWithdrawDiaSourceRecord,
)


class ApdbUpdateRecordTestCase(unittest.TestCase):
    """A test case for ApdbUpdateRecord sub-classes."""

    update_time_ns1 = 2_000_000_000_000_000_000
    update_time_ns2 = 2_000_000_001_000_000_000

    def test_reassign_diasource(self) -> None:
        """Test round-tripping ApdbReassignDiaSourceRecord class."""
        record = ApdbReassignDiaSourceRecord(
            update_time_ns=self.update_time_ns1,
            update_order=0,
            diaSourceId=123456,
            diaObjectId=321,
            ssObjectId=1,
            ssObjectReassocTimeMjdTai=60000.0,
            ra=45.0,
            dec=-45.0,
        )
        record_json = record.to_json()
        record_dict = json.loads(record_json)
        self.assertEqual(
            record_dict,
            {
                "diaSourceId": 123456,
                "diaObjectId": 321,
                "ssObjectId": 1,
                "ssObjectReassocTimeMjdTai": 60000.0,
                "ra": 45.0,
                "dec": -45.0,
                "update_type": "reassign_diasource",
            },
        )

        record2 = ApdbUpdateRecord.from_json(self.update_time_ns1, 0, record_json)
        self.assertIsInstance(record2, ApdbReassignDiaSourceRecord)
        self.assertEqual(record2, record)

    def test_close_diaobject_validity(self) -> None:
        """Test round-tripping ApdbCloseDiaObjectValidityRecord class."""
        record = ApdbCloseDiaObjectValidityRecord(
            update_time_ns=self.update_time_ns1,
            update_order=0,
            diaObjectId=321,
            validityEndMjdTai=60000.0,
            nDiaSources=None,
            ra=45.0,
            dec=-45.0,
        )
        record_json = record.to_json()
        record_dict = json.loads(record_json)
        self.assertEqual(
            record_dict,
            {
                "diaObjectId": 321,
                "validityEndMjdTai": 60000.0,
                "nDiaSources": None,
                "ra": 45.0,
                "dec": -45.0,
                "update_type": "close_diaobject_validity",
            },
        )

        record2 = ApdbUpdateRecord.from_json(self.update_time_ns1, 0, record_json)
        self.assertIsInstance(record2, ApdbCloseDiaObjectValidityRecord)
        self.assertEqual(record2, record)

    def test_update_n_dia_sources(self) -> None:
        """Test round-tripping ApdbUpdateNDiaSourcesRecord class."""
        record = ApdbUpdateNDiaSourcesRecord(
            update_time_ns=self.update_time_ns1,
            update_order=0,
            diaObjectId=321,
            nDiaSources=6,
            ra=45.0,
            dec=-45.0,
        )
        record_json = record.to_json()
        record_dict = json.loads(record_json)
        self.assertEqual(
            record_dict,
            {
                "diaObjectId": 321,
                "nDiaSources": 6,
                "ra": 45.0,
                "dec": -45.0,
                "update_type": "update_n_dia_sources",
            },
        )

        record2 = ApdbUpdateRecord.from_json(self.update_time_ns1, 0, record_json)
        self.assertIsInstance(record2, ApdbUpdateNDiaSourcesRecord)
        self.assertEqual(record2, record)

    def test_withdraw_diasource(self) -> None:
        """Test round-tripping ApdbWithdrawDiaSourceRecord class."""
        record = ApdbWithdrawDiaSourceRecord(
            update_time_ns=self.update_time_ns1,
            update_order=0,
            diaSourceId=123456,
            diaObjectId=321,
            timeWithdrawnMjdTai=61000.0,
            ra=45.0,
            dec=-45.0,
        )
        record_json = record.to_json()
        record_dict = json.loads(record_json)
        self.assertEqual(
            record_dict,
            {
                "diaSourceId": 123456,
                "diaObjectId": 321,
                "timeWithdrawnMjdTai": 61000.0,
                "ra": 45.0,
                "dec": -45.0,
                "update_type": "withdraw_diasource",
            },
        )

        record2 = ApdbUpdateRecord.from_json(self.update_time_ns1, 0, record_json)
        self.assertIsInstance(record2, ApdbWithdrawDiaSourceRecord)
        self.assertEqual(record2, record)

    def test_withdraw_diaforcedsource(self) -> None:
        """Test round-tripping ApdbWithdrawDiaForcedSourceRecord class."""
        record = ApdbWithdrawDiaForcedSourceRecord(
            update_time_ns=self.update_time_ns1,
            update_order=0,
            diaObjectId=1234,
            visit=555,
            detector=123,
            timeWithdrawnMjdTai=61000.0,
            ra=45.0,
            dec=-45.0,
        )
        record_json = record.to_json()
        record_dict = json.loads(record_json)
        self.assertEqual(
            record_dict,
            {
                "diaObjectId": 1234,
                "visit": 555,
                "detector": 123,
                "timeWithdrawnMjdTai": 61000.0,
                "ra": 45.0,
                "dec": -45.0,
                "update_type": "withdraw_diaforcedsource",
            },
        )

        record2 = ApdbUpdateRecord.from_json(self.update_time_ns1, 0, record_json)
        self.assertIsInstance(record2, ApdbWithdrawDiaForcedSourceRecord)
        self.assertEqual(record2, record)

    def test_ordering(self) -> None:
        """Test ordering of records."""
        record1 = ApdbReassignDiaSourceRecord(
            update_time_ns=self.update_time_ns1,
            update_order=0,
            diaSourceId=1,
            diaObjectId=321,
            ssObjectId=1,
            ssObjectReassocTimeMjdTai=60000.0,
            ra=45.0,
            dec=-45.0,
        )
        record2 = ApdbWithdrawDiaSourceRecord(
            update_time_ns=self.update_time_ns1,
            update_order=1,
            diaSourceId=123456,
            diaObjectId=321,
            timeWithdrawnMjdTai=61000.0,
            ra=45.0,
            dec=-45.0,
        )
        record3 = ApdbReassignDiaSourceRecord(
            update_time_ns=self.update_time_ns1,
            update_order=3,
            diaSourceId=2,
            diaObjectId=3,
            ssObjectId=3,
            ssObjectReassocTimeMjdTai=60000.0,
            ra=45.0,
            dec=-45.0,
        )
        record4 = ApdbWithdrawDiaSourceRecord(
            update_time_ns=self.update_time_ns2,
            update_order=0,
            diaSourceId=123456,
            diaObjectId=321,
            timeWithdrawnMjdTai=61000.0,
            ra=45.0,
            dec=-45.0,
        )
        record5 = ApdbWithdrawDiaForcedSourceRecord(
            update_time_ns=self.update_time_ns2,
            update_order=1,
            diaObjectId=1234,
            visit=555,
            detector=123,
            timeWithdrawnMjdTai=61000.0,
            ra=45.0,
            dec=-45.0,
        )

        unordered = [record5, record3, record1, record4, record2]
        ordered = sorted(unordered)
        self.assertTrue(ordered[0] < ordered[1] < ordered[2] < ordered[3] < ordered[4])
        self.assertIs(ordered[0], record1)
        self.assertIs(ordered[1], record2)
        self.assertIs(ordered[2], record3)
        self.assertIs(ordered[3], record4)
        self.assertIs(ordered[4], record5)

        with self.assertRaisesRegex(TypeError, "ordering is not supported between"):
            record1 < (1, 2, 3)


if __name__ == "__main__":
    unittest.main()
