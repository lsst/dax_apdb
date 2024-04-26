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

import unittest
from collections.abc import Mapping
from typing import Any

from lsst.dax.apdb import monitor, timer


class _TestHandler(monitor.MonHandler):
    """Implementation of monitoring handler used in unit tests."""

    name: str
    tags: Mapping
    values: Mapping
    agent_name: str

    def handle(
        self, name: str, timestamp: float, tags: monitor._TagsType, values: Mapping[str, Any], agent_name: str
    ) -> None:
        self.name = name
        self.tags = tags
        self.values = values
        self.agent_name = agent_name


class MonitorTestCase(unittest.TestCase):
    """Unit tests for monitoring classes."""

    def setUp(self) -> None:
        # MonService is a singleton wit global/shared test, need to reset it
        # for each new test.
        monsvc = monitor.MonService()
        monsvc._handlers = []
        monsvc._context_tags = None
        monsvc._filters = []

    def test_simple(self) -> None:
        """Test few simple calls."""
        handler = _TestHandler()
        monitor.MonService().add_handler(handler)

        agent = monitor.MonAgent("007")
        agent.add_record("mon", values={"quantity": 1})

        self.assertEqual(handler.name, "mon")
        self.assertEqual(handler.tags, {})
        self.assertEqual(handler.values, {"quantity": 1})
        self.assertEqual(handler.agent_name, "007")

    def test_two_handlers(self) -> None:
        """Test few simple calls with two handlers."""
        handler1 = _TestHandler()
        monitor.MonService().add_handler(handler1)
        handler2 = _TestHandler()
        monitor.MonService().add_handler(handler2)

        agent = monitor.MonAgent("007")
        agent.add_record("mon", values={"quantity": 1}, tags={"tag1": "atag"})

        self.assertEqual(handler1.name, "mon")
        self.assertEqual(handler1.tags, {"tag1": "atag"})
        self.assertEqual(handler1.values, {"quantity": 1})
        self.assertEqual(handler1.agent_name, "007")
        self.assertEqual(handler2.name, "mon")
        self.assertEqual(handler2.tags, {"tag1": "atag"})
        self.assertEqual(handler2.values, {"quantity": 1})
        self.assertEqual(handler2.agent_name, "007")

    def test_tag_context(self) -> None:
        """Test tag context."""
        handler = _TestHandler()
        monitor.MonService().add_handler(handler)

        agent = monitor.MonAgent("007")

        with agent.context_tags({"tag1": "atag"}):
            agent.add_record("mon", values={"quantity": 1})

        self.assertEqual(handler.name, "mon")
        self.assertEqual(handler.tags, {"tag1": "atag"})
        self.assertEqual(handler.values, {"quantity": 1})
        self.assertEqual(handler.agent_name, "007")

    def test_logging_handler(self) -> None:
        """Test logging handler."""
        handler = monitor.LoggingMonHandler("logger")
        monitor.MonService().add_handler(handler)

        agent = monitor.MonAgent("007")

        with self.assertLogs("logger", level="INFO") as cm:
            # Need to use fixed timestamps here to be able to compare strings.
            agent.add_record("mon", values={"quantity": 1}, tags={"tag1": "atag1"}, timestamp=100.0)
            agent.add_record("mon", values={"quantity": 2}, tags={"tag1": "atag2"}, timestamp=101.0)
        self.assertEqual(
            cm.output,
            [
                'INFO:logger:{"name": "mon", "timestamp": 100.0, '
                '"tags": {"tag1": "atag1"}, "values": {"quantity": 1}, "source": "007"}',
                'INFO:logger:{"name": "mon", "timestamp": 101.0, '
                '"tags": {"tag1": "atag2"}, "values": {"quantity": 2}, "source": "007"}',
            ],
        )

    def test_timer(self) -> None:
        """Test monitoring output via Timer."""
        handler = _TestHandler()
        monitor.MonService().add_handler(handler)

        agent = monitor.MonAgent("007")
        with agent.context_tags({"tag1": "atag"}):
            with timer.Timer("timer", agent):
                pass
        self.assertEqual(handler.name, "timer")
        self.assertEqual(handler.tags, {"tag1": "atag"})
        self.assertEqual(set(handler.values), {"real", "user", "sys"})
        self.assertEqual(handler.agent_name, "007")

    def test_filters(self) -> None:
        """Test agent filtering."""
        handler = _TestHandler()
        monitor.MonService().add_handler(handler)

        # Note that unlike in the `logging` there is no parent/child relation
        # between agent, disabling "lsst" will not disable two others.
        agent1 = monitor.MonAgent("lsst.agent1")
        agent2 = monitor.MonAgent("lsst.agent2")
        agent3 = monitor.MonAgent("lsst")

        # By default nothing is filtered
        agent1.add_record("mon1", values={"quantity": 1})
        self.assertEqual(handler.name, "mon1")
        self.assertEqual(handler.values["quantity"], 1)
        agent2.add_record("mon2", values={"quantity": 2})
        self.assertEqual(handler.name, "mon2")
        self.assertEqual(handler.values["quantity"], 2)
        agent3.add_record("mon3", values={"quantity": 3})
        self.assertEqual(handler.name, "mon3")
        self.assertEqual(handler.values["quantity"], 3)

        # Disable one of them.
        monitor.MonService().set_filters(["-lsst"])
        agent1.add_record("mon1", values={"quantity": 1})
        self.assertEqual(handler.name, "mon1")
        self.assertEqual(handler.values["quantity"], 1)
        agent2.add_record("mon2", values={"quantity": 2})
        self.assertEqual(handler.name, "mon2")
        self.assertEqual(handler.values["quantity"], 2)
        agent3.add_record("mon3", values={"quantity": 3})
        self.assertEqual(handler.name, "mon2")
        self.assertEqual(handler.values["quantity"], 2)

        # Disable all except one.
        monitor.MonService().set_filters(["+lsst.agent1", "-any"])
        agent1.add_record("mon1", values={"quantity": 1})
        self.assertEqual(handler.name, "mon1")
        self.assertEqual(handler.values["quantity"], 1)
        agent2.add_record("mon2", values={"quantity": 2})
        self.assertEqual(handler.name, "mon1")
        self.assertEqual(handler.values["quantity"], 1)
        agent3.add_record("mon3", values={"quantity": 3})
        self.assertEqual(handler.name, "mon1")
        self.assertEqual(handler.values["quantity"], 1)

        # Plus sign is optional in the rule.
        monitor.MonService().set_filters(["+lsst.agent1", "lsst.agent2", "-lsst"])
        agent1.add_record("mon1", values={"quantity": 1})
        self.assertEqual(handler.name, "mon1")
        self.assertEqual(handler.values["quantity"], 1)
        agent2.add_record("mon2", values={"quantity": 2})
        self.assertEqual(handler.name, "mon2")
        self.assertEqual(handler.values["quantity"], 2)
        agent3.add_record("mon3", values={"quantity": 3})
        self.assertEqual(handler.name, "mon2")
        self.assertEqual(handler.values["quantity"], 2)


if __name__ == "__main__":
    unittest.main()
