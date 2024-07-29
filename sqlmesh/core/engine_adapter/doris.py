from __future__ import annotations

import logging
import typing as t

from sqlglot import exp

from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter
from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
)

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import TableName
    from sqlmesh.core.engine_adapter._typing import QueryOrDF

logger = logging.getLogger(__name__)


class DistributedByRandom(exp.Property):
    arg_types = {"buckets": False}


@set_catalog()
class DorisEngineAdapter(MySQLEngineAdapter):
    DIALECT = "doris"

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expression]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        """
        Doris does not support CREATE OR REPLACE VIEW.
        So we have to drop the view first if it exists. This is not ideal but it is the only way to do it.

        Reference: https://doris.apache.org/docs/dev/sql-manual/sql-statements/Data-Definition-Statements/Create/CREATE-VIEW/
        """
        with self.transaction():
            if replace:
                self.drop_view(view_name, materialized=materialized)
            super().create_view(
                view_name,
                query_or_df,
                columns_to_types=columns_to_types,
                replace=False,
                materialized=materialized,
                table_description=table_description,
                column_descriptions=column_descriptions,
                view_properties=view_properties,
                **create_kwargs,
            )
