from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
)

if t.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class DistributedByRandom(exp.Property):
    arg_types = {"buckets": False}


@set_catalog()
class DorisEngineAdapter(MySQLEngineAdapter):
    DIALECT = "doris"

    # def _build_table_properties_exp(
    #     self,
    #     catalog_name: t.Optional[str] = None,
    #     storage_format: t.Optional[str] = None,
    #     partitioned_by: t.Optional[t.List[exp.Expression]] = None,
    #     partition_interval_unit: t.Optional[IntervalUnit] = None,
    #     clustered_by: t.Optional[t.List[str]] = None,
    #     table_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
    #     columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
    #     table_description: t.Optional[str] = None,
    #     table_kind: t.Optional[str] = None,
    # ) -> t.Optional[exp.Properties]:
    #     properties: t.List[exp.Expression] = []

    #     properties.append(exp.SettingsProperty(this="OLAP"))

    #     if properties:
    #         return exp.Properties(expressions=properties)
    #     return None
