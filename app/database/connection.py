import logging

from gremlin_python.driver.aiohttp.transport import AiohttpTransport
from gremlin_python.driver.driver_remote_connection import DriverRemoteConnection  # noqa
from gremlin_python.process.anonymous_traversal import traversal  # noqa
from gremlin_python.process.graph_traversal import GraphTraversal, GraphTraversalSource, __  # noqa

from .bulk_query_executor import BulkQueryExecutor


class Connection:
    _instance = None

    @classmethod
    def get_instance(cls):
        return cls._instance

    def __init__(
        self,
        url: str,
    ) -> None:
        self._connection = DriverRemoteConnection(
            url=url,
            traversal_source="g",
            transport_factory=lambda: AiohttpTransport(call_from_event_loop=True),
        )
        self._traversal_source = None
        self._query_executor = None
        self._logger = logging.getLogger(self.__class__.__name__)

        Connection._instance = self

    @property
    def traversal_source(self) -> GraphTraversalSource:
        if (self._connection.is_closed()) or (self._traversal_source is None):
            self._traversal_source = traversal().with_remote(self._connection)
        return self._traversal_source

    @property
    def query_executor(self) -> BulkQueryExecutor:
        if (self._connection.is_closed()) or (self._query_executor is None):
            self._query_executor = BulkQueryExecutor(self.traversal_source)
        return self._query_executor

    def log_graph_status(self):
        g = self.traversal_source
        vertex_count = g.V().count().next()
        edge_count = g.E().count().next()
        self._logger.warning(f"Graph status: {vertex_count} vertices, {edge_count} edges.")
