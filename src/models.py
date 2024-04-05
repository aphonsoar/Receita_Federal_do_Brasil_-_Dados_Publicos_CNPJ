from typing import NamedTuple, List

from sqlalchemy import create_engine
from psycopg2 import connect

class Database(NamedTuple):
    engine: create_engine
    conn: connect

class TableInfo(NamedTuple):
    label: str
    table_name: str
    columns: List[str]
    encoding: str
    transform_map: callable
