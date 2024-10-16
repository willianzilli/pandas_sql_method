from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.connectors import Connector
from pandas.io.sql import SQLTable
from typing import (
    Any,
    Iterable
)

def insert_on_duplicate(modelClass: Any, table: SQLTable, conn: Connector, columns: list, ivalues: Iterable[tuple]) -> None:
    insert_stmt = insert(table.table).values(list(ivalues))
    tableModel = modelClass.Base.metadata.tables[table.table.fullname]
    primary_keys = tableModel.primary_key.columns.keys()

    for mapper in modelClass.Base.registry.mappers:
        modelInstance = mapper.class_
        onConflictSet = dict()

        if modelInstance.__tablename__ == table.table.name:
            onConflictColumns = columns

            if hasattr(modelInstance, 'OnConflictColumns'):
                onConflictColumns = modelInstance.OnConflictColumns()
            
            for column in onConflictColumns:
                if (column not in primary_keys):
                    onConflictSet[column] = insert_stmt.excluded[column]

            break

    on_duplicate_key_stmt = insert_stmt.on_conflict_do_update(
        index_elements=primary_keys,
        set_=onConflictSet
    )
    conn.execute(on_duplicate_key_stmt)
