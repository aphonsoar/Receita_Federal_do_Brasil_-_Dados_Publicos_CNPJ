from sqlalchemy import (
  Column, Integer, String, TIMESTAMP, create_engine
)
from typing import NamedTuple, List, Optional, Generic, TypeVar
from pydantic import BaseModel, Field
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime

from functools import reduce

# Define the base class for table models
Base = declarative_base()

T = TypeVar('T')

class AuditDBSchema(BaseModel, Generic[T]):
    audi_id: int = Field(..., description="Unique identifier for the audit entry.")
    audi_filename: str = Field(..., description="Filename associated with the audit entry.")
    audi_source_updated_at: Optional[datetime] = Field(
        None, description="Timestamp of the last source update."
    )
    audi_created_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry creation."
    )
    audi_downloaded_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry download."
    )
    audi_processed_at: Optional[datetime] = Field(
        None, description="Timestamp of the audit entry processing."
    )
    audi_inserted_at: datetime = Field(
        ..., description="Timestamp of the audit entry insertion."
    )

class AuditDB(Base):
    """
    SQLAlchemy model for the audit table.
    """
    __tablename__ = 'audit'

    audi_id = Column(Integer, primary_key=True)
    audi_filename = Column(String(255), nullable=False)
    audi_file_size_bytes = Column(Integer, nullable=False)
    audi_source_updated_at = Column(TIMESTAMP, nullable=True)
    audi_created_at = Column(TIMESTAMP, nullable=True)
    audi_downloaded_at = Column(TIMESTAMP, nullable=True)
    audi_processed_at = Column(TIMESTAMP, nullable=True)
    audi_inserted_at = Column(TIMESTAMP, nullable=True)

    @property
    def is_precedence_met(self) -> bool:
        """
        Checks if the current timestamp (audi_evaluated_at) is greater than the previous timestamps in order.
        
        Returns:
            bool: True if the precedence is met, False otherwise.
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
            previous_t = previous_timestamps[0:index]

            if index > 0:
                greater_than_map = lambda a: a < current_timestamp
                are_greater = map(greater_than_map, previous_t)
                this_is_met = reduce(and_map, are_greater)

                is_met = is_met and this_is_met
            
        return is_met
    
    def __get_pydantic_core_schema__(self):
        """
        Defines the Pydantic schema for AuditDB.
        """
        return AuditDBSchema
    
    def __repr__(self):
        source_updated_at=f"audi_source_updated_at={self.audi_source_updated_at}"
        created_at=f"audi_created_at={self.audi_created_at}"
        downloaded_at=f"audi_downloaded_at={self.audi_downloaded_at}"
        processed_at=f"audi_processed_at={self.audi_processed_at}"
        inserted_at=f"audi_inserted_at={self.audi_inserted_at}"
        timestamps=f"{source_updated_at}, {created_at}, {downloaded_at}, {processed_at}, {inserted_at}"
        
        file_info = f"audi_filename={self.audi_filename}, audi_file_size_bytes={self.audi_file_size_bytes}"
        args=f"audi_id={self.audi_id}, {file_info}, {timestamps}"
        
        return f"AuditDB({args})"