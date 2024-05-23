from typing import NamedTuple, List, Optional
from datetime import datetime

from sqlalchemy import Column, Integer, String, TIMESTAMP, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel
from functools import reduce

# Define the base class for table models
Base = declarative_base()

class Audit(BaseModel):
  """
  Pydantic model representing an audit entry.
  """
  audi_id: int
  audi_filename: str
  audi_source_updated_at: datetime
  audi_created_at: datetime
  audi_downloaded_at: datetime
  audi_processed_at: datetime
  audi_inserted_at: datetime

class AuditDB(Base):
    """
    SQLAlchemy model for the audit table.
    """
    __tablename__ = 'audit'

    audi_id = Column(Integer, primary_key=True)
    audi_filename = Column(String(255), nullable=False)
    audi_source_updated_at = Column(TIMESTAMP, nullable=True)
    audi_created_at = Column(TIMESTAMP, nullable=True)
    audi_downloaded_at = Column(TIMESTAMP, nullable=True)
    audi_processed_at = Column(TIMESTAMP, nullable=True)
    audi_inserted_at = Column(TIMESTAMP, nullable=True)

    @property
    def is_precedence_met(self) -> bool:
      """
      Checks if the current timestamp (audi_evaluated_at) is greater than the previous timestamps in order.
      """
      previous_timestamps = [
          self.audi_created_at,
          self.audi_downloaded_at,
          self.audi_processed_at,
          self.audi_inserted_at,
      ]
      
      is_met = True
      and_map = lambda a, b: a and b
      
      for index, current_timestamp in enumerate(previous_timestamps):
          greater_than_map = lambda a: a < current_timestamp
          are_greater=map(greater_than_map, previous_timestamps[0:index+1])
          this_is_met = reduce(and_map, are_greater)
          
          is_met = is_met and this_is_met

      return is_met
    
    def __repr__(self):
      source_updated_at=f"audi_source_updated_at={self.audi_source_updated_at}"
      created_at=f"audi_created_at={self.audi_created_at}"
      downloaded_at=f"audi_downloaded_at={self.audi_downloaded_at}"
      processed_at=f"audi_processed_at={self.audi_processed_at}"
      inserted_at=f"audi_inserted_at={self.audi_inserted_at}"
      timestamps=f"{source_updated_at}, {created_at}, {downloaded_at}, {processed_at}, {inserted_at}"
      args=f"audi_id={self.audi_id}, audi_filename={self.audi_filename}, {timestamps}"
      
      return f"<AuditDB({args})>"

class CNPJFile(BaseModel):
  """
  Pydantic model representing a CNPJ file.
  """
  filename: str
  updated_at: datetime

class Database(NamedTuple):
  """
  Database connection and session management object (optional).
  """
  engine: callable
  session_maker: callable


class TableInfo(NamedTuple):
  """Represents information about a table.

  Args:
    label (str): The label of the table.
    table_name (str): The name of the table.
    columns (List[str]): The list of column names in the table.
    encoding (str): The encoding used for the table.
    transform_map (callable): A callable object used to transform the table.

  Attributes:
    label (str): The label of the table.
    table_name (str): The name of the table.
    columns (List[str]): The list of column names in the table.
    encoding (str): The encoding used for the table.
    transform_map (callable): A callable object used to transform the table.
  """
  
  label: str
  table_name: str
  columns: List[str]
  encoding: str
  transform_map: callable
