from __future__ import annotations

import logging
import typing as t


from sqlmesh.core.engine_adapter.shared import (
    set_catalog,
)

from sqlmesh.core.engine_adapter.mysql import MySQLEngineAdapter

if t.TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(MySQLEngineAdapter):
    DIALECT = "doris"
