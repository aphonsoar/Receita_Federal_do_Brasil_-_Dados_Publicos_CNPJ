from typing import NamedTuple, List, Dict
from datetime import datetime

from pydantic import BaseModel, Field
from functools import reduce

from database.models import AuditDB

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


class FileInfo(BaseModel):
  """
  Pydantic model representing a CNPJ file.

  Attributes:
    filename (str): The name of the CNPJ file.
    updated_at (datetime): The date and time when the CNPJ file was last updated.
  """
  filename: str
  updated_at: datetime
  file_size: int = 0

class AuditMetadata(BaseModel):
  """
  Represents the metadata for auditing purposes.

  Attributes:
    audit_list (List): A list of audit items.
    tablename_to_zipfile_to_files (Dict): A dictionary mapping table names to zip files and their associated files.
  """

  audit_list: List
  tablename_to_zipfile_to_files: Dict
  
  def __repr__(self) -> str:
    args=f'audit_list={self.audit_list}, tablename_to_zipfile_to_files={self.tablename_to_zipfile_to_files}'
    return f"AuditMetadata({args})"

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
