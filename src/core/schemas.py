from typing import NamedTuple, List, Dict
from datetime import datetime
from typing import Tuple

from pydantic import BaseModel

from utils.misc import normalize_filename

class Audit(BaseModel):
  """
  Pydantic model representing an audit entry.

  Attributes:
    audi_id (int): The ID of the audit entry.
    audi_table_name (str): The table name associated with the audit entry.
    audi_source_updated_at (datetime): The datetime when the source was last updated.
    audi_created_at (datetime): The datetime when the audit entry was created.
    audi_downloaded_at (datetime): The datetime when the audit entry was downloaded.
    audi_processed_at (datetime): The datetime when the audit entry was processed.
    audi_inserted_at (datetime): The datetime when the audit entry was inserted.

  """
  audi_id: int
  audi_table_name: str
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


class FileGroupInfo(BaseModel):
  """
  Pydantic model representing a group of CNPJ files.

  Attributes:
    normalized_name (str): The normalized name of the CNPJ file.
    original_names (List[str]): The list of original names of the CNPJ files.
    date_range (Tuple[datetime, datetime]): The date range of the CNPJ files.
  """
  name: str
  elements: List[str]
  date_range: Tuple[datetime, datetime]
  table_name: str
  size_bytes: int = 0

  def date_diff(self) -> float:
    """
    Returns the difference in days between the start and end dates of the group.

    Returns:
      float: The difference in days between the start and end dates of the group.
    """
    start, end = self.date_range
    return (end - start).days
  

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

    Attributes:
      label (str): The label of the table.
      group (str): The zip group of the table.
      table_name (str): The name of the table.
      columns (List[str]): The list of column names in the table.
      encoding (str): The encoding used for the table.
      transform_map (callable): A callable object used to transform the table.
      expression (str): The expression used to identify the file belonging to table. 
    """

    label: str
    zip_group: str
    table_name: str
    columns: List[str]
    encoding: str
    transform_map: callable
    expression: str

    def zip_file_belonging_to_table(self, filename: str) -> bool:
        """
        Determines if a file belongs to the table.

        Args:
          filename (str): The name of the file.

        Returns:
          bool: True if the file belongs to the table, False otherwise.
        """
        return normalize_filename(filename) == self.zip_group