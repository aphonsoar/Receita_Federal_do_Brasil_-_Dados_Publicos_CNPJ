from typing import NamedTuple, List

class Database(NamedTuple):
    engine: callable

class TableInfo(NamedTuple):
    label: str
    table_name: str
    columns: List[str]
    encoding: str
    transform_map: callable
