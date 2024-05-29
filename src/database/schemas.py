from typing import NamedTuple

class Database(NamedTuple):
  """
  This class represents a database connection and session management object.
  It contains two attributes:
  
  - engine: A callable that represents the database engine.
  - session_maker: A callable that represents the session maker.
  """
  engine: callable
  session_maker: callable