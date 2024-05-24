from typing import NamedTuple, List, Dict, Optional
from datetime import datetime

from sqlalchemy.orm import sessionmaker
from pydantic import BaseModel, Field
from functools import reduce

from .database import AuditDB

class Audit(BaseModel):
  """
  Pydantic model representing an audit entry.

  Attributes:
    audi_id (int): The ID of the audit entry.
    audi_filename (str): The filename associated with the audit entry.
    audi_source_updated_at (datetime): The datetime when the source was last updated.
    audi_created_at (datetime): The datetime when the audit entry was created.
    audi_downloaded_at (datetime): The datetime when the audit entry was downloaded.
    audi_processed_at (datetime): The datetime when the audit entry was processed.
    audi_inserted_at (datetime): The datetime when the audit entry was inserted.

  """
  audi_id: int
  audi_filename: str
  audi_file_size_bytes: int
  audi_source_updated_at: datetime
  audi_created_at: datetime
  audi_downloaded_at: datetime
  audi_processed_at: datetime
  audi_inserted_at: datetime


class CNPJZipFile(BaseModel):
  """
  Pydantic model representing a CNPJ file.

  Attributes:
    filename (str): The name of the CNPJ file.
    updated_at (datetime): The date and time when the CNPJ file was last updated.
  """
  filename: str
  updated_at: datetime
  
class ZipContent(BaseModel):
  """
  Pydantic model representing a CNPJ file.

  Attributes:
    filename (str): The name of the CNPJ file.
    updated_at (datetime): The date and time when the CNPJ file was last updated.
  """
  filename: str
  zip_filename: str
  file_size: int

class AuditMetadata(BaseModel):
    audit_list: List
    zip_file_dict: Dict
    
class Database(NamedTuple):
  """
  This class represents a database connection and session management object.
  It contains two attributes:
  
  - engine: A callable that represents the database engine.
  - session_maker: A callable that represents the session maker.
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
