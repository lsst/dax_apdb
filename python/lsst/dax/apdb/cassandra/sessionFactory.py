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

__all__ = ["SessionContext", "SessionFactory"]

import logging
from collections.abc import Mapping
from contextlib import ExitStack
from typing import TYPE_CHECKING, Any

# If cassandra-driver is not there the module can still be imported
# but ApdbCassandra cannot be instantiated.
try:
    import cassandra
    import cassandra.query
    from cassandra.auth import AuthProvider, PlainTextAuthProvider
    from cassandra.cluster import EXEC_PROFILE_DEFAULT, Cluster, ExecutionProfile, Session
    from cassandra.policies import AddressTranslator, RoundRobinPolicy, WhiteListRoundRobinPolicy

    CASSANDRA_IMPORTED = True
except ImportError:
    CASSANDRA_IMPORTED = False

from lsst.utils.db_auth import DbAuth, DbAuthNotFoundError

from ..monitor import MonAgent
from ..timer import Timer
from .cassandra_utils import raw_data_factory

if TYPE_CHECKING:
    from .config import ApdbCassandraConfig

_LOG = logging.getLogger(__name__)

_MON = MonAgent(__name__)


def _dump_query(rf: Any) -> None:
    """Dump cassandra query to debug log."""
    _LOG.debug("Cassandra query: %s", rf.query)


if CASSANDRA_IMPORTED:

    class _AddressTranslator(AddressTranslator):
        """Translate internal IP address to external.

        Only used for docker-based setup, not a viable long-term solution.
        """

        def __init__(self, public_ips: tuple[str, ...], private_ips: tuple[str, ...]):
            self._map = dict(zip(private_ips, public_ips))

        def translate(self, private_ip: str) -> str:
            return self._map.get(private_ip, private_ip)


class SessionFactory:
    """Implementation of SessionFactory that uses parameters from Apdb
    configuration.

    Parameters
    ----------
    config : `ApdbCassandraConfig`
        Configuration object.
    """

    def __init__(self, config: ApdbCassandraConfig):
        self._config = config
        self._cluster: Cluster | None = None
        self._session: Session | None = None

    def __del__(self) -> None:
        # Need to call Cluster.shutdown() to avoid warnings.
        if hasattr(self, "_cluster"):
            if self._cluster:
                self._cluster.shutdown()

    def session(self) -> Session:
        """Return Cassandra Session, making new connection if necessary.

        Returns
        -------
        session : `cassandra.cluster.Sesion`
            Cassandra session object.
        """
        if self._session is None:
            self._cluster, self._session = self._make_session()
        return self._session

    def _make_session(self) -> tuple[Cluster, Session]:
        """Make Cassandra session.

        Returns
        -------
        cluster : `cassandra.cluster.Cluster`
            Cassandra Cluster object
        session : `cassandra.cluster.Session`
            Cassandra session object
        """
        addressTranslator: AddressTranslator | None = None
        if self._config.connection_config.private_ips:
            addressTranslator = _AddressTranslator(
                self._config.contact_points, self._config.connection_config.private_ips
            )

        extra_parameters = {
            "idle_heartbeat_interval": 0,
            "idle_heartbeat_timeout": 30,
            "control_connection_timeout": 100,
            "executor_threads": 10,
        }
        extra_parameters.update(self._config.connection_config.extra_parameters)
        with Timer("cluster_connect", _MON):
            cluster = Cluster(
                execution_profiles=self._make_profiles(),
                contact_points=self._config.contact_points,
                port=self._config.connection_config.port,
                address_translator=addressTranslator,
                protocol_version=self._config.connection_config.protocol_version,
                auth_provider=self._make_auth_provider(),
                **extra_parameters,
            )
            session = cluster.connect()

        # Dump queries if debug level is enabled.
        if _LOG.isEnabledFor(logging.DEBUG):
            session.add_request_init_listener(_dump_query)

        # Disable result paging
        session.default_fetch_size = None

        return cluster, session

    def _make_auth_provider(self) -> AuthProvider | None:
        """Make Cassandra authentication provider instance."""
        try:
            dbauth = DbAuth()
        except DbAuthNotFoundError:
            # Credentials file doesn't exist, use anonymous login.
            return None

        empty_username = True
        # Try every contact point in turn.
        for hostname in self._config.contact_points:
            try:
                username, password = dbauth.getAuth(
                    "cassandra",
                    self._config.connection_config.username,
                    hostname,
                    self._config.connection_config.port,
                    self._config.keyspace,
                )
                if not username:
                    # Password without user name, try next hostname, but give
                    # warning later if no better match is found.
                    empty_username = True
                else:
                    return PlainTextAuthProvider(username=username, password=password)
            except DbAuthNotFoundError:
                pass

        if empty_username:
            _LOG.warning(
                f"Credentials file ({dbauth.db_auth_path}) provided password but not "
                "user name, anonymous Cassandra logon will be attempted."
            )

        return None

    def _make_profiles(self) -> Mapping[Any, ExecutionProfile]:
        """Make all execution profiles used in the code."""
        config = self._config
        if config.connection_config.private_ips:
            loadBalancePolicy = WhiteListRoundRobinPolicy(hosts=config.contact_points)
        else:
            loadBalancePolicy = RoundRobinPolicy()

        read_tuples_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.connection_config.read_consistency),
            request_timeout=config.connection_config.read_timeout,
            row_factory=cassandra.query.tuple_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        read_named_tuples_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.connection_config.read_consistency),
            request_timeout=config.connection_config.read_timeout,
            row_factory=cassandra.query.named_tuple_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        read_raw_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.connection_config.read_consistency),
            request_timeout=config.connection_config.read_timeout,
            row_factory=raw_data_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        # Profile to use with select_concurrent to return raw data (columns and
        # rows)
        read_raw_multi_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.connection_config.read_consistency),
            request_timeout=config.connection_config.read_timeout,
            row_factory=raw_data_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        # Profile to use with select_concurrent to return raw data,
        # this also has very long timeout, to be be use for querying
        # DiaObjectDedup table that can return a lot of data.
        read_raw_multi_dedup_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.connection_config.read_consistency),
            request_timeout=3600.0,
            row_factory=raw_data_factory,
            load_balancing_policy=loadBalancePolicy,
        )
        write_profile = ExecutionProfile(
            consistency_level=getattr(cassandra.ConsistencyLevel, config.connection_config.write_consistency),
            request_timeout=config.connection_config.write_timeout,
            load_balancing_policy=loadBalancePolicy,
        )
        # To replace default DCAwareRoundRobinPolicy
        default_profile = ExecutionProfile(
            load_balancing_policy=loadBalancePolicy,
        )
        return {
            "read_tuples": read_tuples_profile,
            "read_named_tuples": read_named_tuples_profile,
            "read_raw": read_raw_profile,
            "read_raw_multi": read_raw_multi_profile,
            "read_raw_multi_dedup": read_raw_multi_dedup_profile,
            "write": write_profile,
            EXEC_PROFILE_DEFAULT: default_profile,
        }


class SessionContext(ExitStack):
    """Context manager for creating short-lived Cassandra sessions.

    Parameters
    ----------
    config : `ApdbCassandraConfig`
        Configuration object.
    """

    def __init__(self, config: ApdbCassandraConfig):
        super().__init__()
        self._session_factory = SessionFactory(config)

    def __enter__(self) -> Session:
        super().__enter__()
        cluster, session = self._session_factory._make_session()
        self.enter_context(cluster)
        self.enter_context(session)
        return session
