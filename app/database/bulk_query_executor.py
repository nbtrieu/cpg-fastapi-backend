# put this whole folder in /deps
from typing import Dict, Optional

import pandas as pd
from gremlin_python.process.graph_traversal import GraphTraversalSource, __  # noqa
from gremlin_python.process.traversal import (  # noqa
    Barrier,
    Bindings,
    Cardinality,
    CardinalityValue,
    Column,
    Direction,
    Operator,
    Order,
    P,
    Pop,
    Scope,
    T,
    TextP,
    WithOptions,
)


class BulkQueryExecutor:
    def __init__(self, traversal_source: GraphTraversalSource, max_query_count: int = 100) -> None:
        self._max_query_count = max_query_count
        self._traversal_source = traversal_source
        self._traversal = self._traversal_source.get_graph_traversal()
        self._query_counter = 0

    def _auto_execute(self):
        if self._query_counter >= self._max_query_count and self._query_counter > 0:
            self._traversal.iterate()
            self._traversal = self._traversal_source.get_graph_traversal()
            self._query_counter = 0

    def force_execute(self):
        if self._query_counter > 0:
            self._traversal.iterate()
            self._traversal = self._traversal_source.get_graph_traversal()
            self._query_counter = 0

    def add_vertex(
        self,
        label: str,
        vertex_id: Optional[str] = None,
        properties: Optional[Dict] = None,
    ):
        self._traversal = self._traversal.add_v(label)
        if vertex_id is not None:
            if not isinstance(vertex_id, str):
                raise TypeError("Vertex ID must be a string")
            self._traversal = self._traversal.property(T.id, vertex_id)

        if properties is not None and isinstance(properties, dict):
            for key in properties:
                if isinstance(properties[key], set) or isinstance(properties[key], list):
                    for item in properties[key]:
                        if pd.notna(item):
                            self._traversal = self._traversal.property(Cardinality.set_, key, item)
                elif pd.notna(properties[key]):
                    self._traversal = self._traversal.property(
                        Cardinality.single, key, properties[key]
                    )
        self._query_counter += 1
        self._auto_execute()

    def add_edge(
        self,
        source_id: str,
        dest_id: str,
        label: str,
        edge_id: Optional[str] = None,
        properties: Optional[Dict] = None,
    ):
        self._traversal = self._traversal.V(source_id).add_e(label).to(__.V(dest_id))

        if edge_id is not None:
            if not isinstance(edge_id, str):
                raise TypeError("Edge ID must be a string")
            self._traversal = self._traversal.property(T.id, edge_id)

        if properties is not None and isinstance(properties, dict):
            for key in properties:
                if isinstance(properties[key], set) or isinstance(properties[key], list):
                    for item in properties[key]:
                        if pd.notna(item):
                            self._traversal = self._traversal.property(Cardinality.set_, key, item)
                elif pd.notna(properties[key]):
                    self._traversal = self._traversal.property(
                        Cardinality.single, key, properties[key]
                    )
        self._query_counter += 1
        self._auto_execute()

    def add_edge_if_not_exist(
        self,
        source_id: str,
        dest_id: str,
        label: str,
        properties: Optional[Dict] = None,
    ):
        add_traversal = __.add_e(label).from_("source_node")
        if properties is not None and isinstance(properties, dict):
            for key in properties:
                if pd.notna(properties[key]):
                    add_traversal = add_traversal.property(key, properties[key])

        self._traversal = (
            self._traversal.V(source_id)
            .as_("source_node")
            .V(dest_id)
            .coalesce(__.in_e(label).where(__.out_v().as_("source_node")), add_traversal)
        )
        self._query_counter += 1
        self._auto_execute()
