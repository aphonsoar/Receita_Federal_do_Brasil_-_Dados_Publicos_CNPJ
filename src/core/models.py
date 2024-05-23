from typing import NamedTuple, List

class Database(NamedTuple):
    """
    Represents a database connection.

    Attributes:
        engine (callable): The engine used for the database connection.
    """
    engine: callable

class TableInfo(NamedTuple):
    label: str
    table_name: str
    columns: List[str]
    encoding: str
    transform_map: callable
