"""
SQLAlchemy ORM models for the CNPJ database tables.

This module defines the structure of each table in the database using SQLAlchemy's
declarative base. It includes models for the main data tables (Empresa, Estabelecimento, etc.),
auxiliary/lookup tables (CNAE, Municipio, etc.), and an AuditDB table for tracking ETL processes.

Additionally, it includes a Pydantic schema (`AuditDBSchema`) which can be used for
data validation or as an interface layer before converting to the `AuditDB` SQLAlchemy model.
"""
from sqlalchemy import (
    Column, BigInteger, String, TIMESTAMP, JSON, Text, Float, Index
)

from sqlalchemy.dialects.postgresql import UUID
from typing import Optional, Generic, TypeVar, List, Any
from pydantic import BaseModel, Field
from datetime import datetime
from uuid import uuid4


from functools import reduce
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

T = TypeVar('T') # Generic type variable, its usage in AuditDBSchema seems minimal here.

class AuditDBSchema(BaseModel, Generic[T]):
    """
    Pydantic schema for AuditDB data.

    This schema can be used for validating data before creating an AuditDB SQLAlchemy
    model instance, or for serializing AuditDB instances to a defined format (e.g., APIs).
    The Generic[T] is present but not actively used by specific fields in this schema.
    """
    audi_id: str = Field(default_factory=uuid4, description="Unique identifier for the audit entry.")
    audi_table_name: str = Field(..., description="Name of the table this audit entry pertains to.")
    audi_filenames: Optional[List[str]] = Field(
        None, description="List of files associated to given table."
    )
    audi_file_size_bytes: Optional[float] = Field(
        None, description="Total size of files respective to given table."
    )
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
    audi_inserted_at: Optional[datetime] = Field(
        None, description="Timestamp of when data was successfully inserted into the target table."
    )

    def to_audit_db(self) -> 'AuditDB':
        """
        Converts this Pydantic schema instance to an AuditDB SQLAlchemy model instance.

        Returns:
            AuditDB: An instance of the AuditDB SQLAlchemy model, populated with data
                     from this schema instance.
        """
        return AuditDB(
            audi_id=self.audi_id, # type: ignore # UUID is handled by SQLAlchemy's TypeDecorator
            audi_table_name=self.audi_table_name,
            audi_filenames=self.audi_filenames,
            audi_file_size_bytes=self.audi_file_size_bytes,
            audi_source_updated_at=self.audi_source_updated_at,
            audi_created_at=self.audi_created_at,
            audi_downloaded_at=self.audi_downloaded_at,
            audi_processed_at=self.audi_processed_at,
            audi_inserted_at=self.audi_inserted_at,
        )

class AuditDB(Base, Generic[T]):
    """
    SQLAlchemy model for the audit table.
    """
    __tablename__ = 'audit'

    audi_id = Column(UUID(as_uuid=True), primary_key=True)
    audi_table_name = Column(String(255), nullable=False)
    audi_filenames = Column(JSON, nullable=False)
    audi_file_size_bytes = Column(BigInteger, nullable=True)
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
                greater_than_map = lambda a: a <= current_timestamp

                this_is_met = reduce(and_map, map(greater_than_map, previous_t))

                is_met = is_met and this_is_met
        
        return is_met
    
    def __get_pydantic_core_schema__(self, source_type: Any, handler: Any) -> Any: # Added type hints for clarity
        """
        Pydantic v2 integration method.
        Allows Pydantic to understand how to validate or serialize this SQLAlchemy model.
        It effectively points to the AuditDBSchema.
        """
        from pydantic_core import core_schema
        # This tells Pydantic to use AuditDBSchema for (de)serialization and validation
        return core_schema.model_schema(AuditDBSchema, ref=f"AuditDB_SQLAlchemyModel")


    def __repr__(self) -> str:
        """
        Provides a developer-friendly string representation of the AuditDB instance.
        """
        source_updated_at=f"audi_source_updated_at={self.audi_source_updated_at!r}" # Use !r for repr of contents
        created_at=f"audi_created_at={self.audi_created_at!r}"
        downloaded_at=f"audi_downloaded_at={self.audi_downloaded_at!r}"
        processed_at=f"audi_processed_at={self.audi_processed_at!r}"
        inserted_at=f"audi_inserted_at={self.audi_inserted_at!r}"
        timestamps=f"{source_updated_at}, {created_at}, {downloaded_at}, {processed_at}, {inserted_at}"
        
        table_name=f"audi_table_name={self.audi_table_name!r}"
        file_size=f"audi_file_size_bytes={self.audi_file_size_bytes!r}"
        filenames=f"audi_filenames={self.audi_filenames!r}" # !r for list representation
        file_info = f"{table_name}, {filenames}, {file_size}"
        args=f"audi_id={self.audi_id!r}, {file_info}, {timestamps}"
        
        return f"AuditDB({args})"

class Empresa(Base):
    """SQLAlchemy model for the 'empresa' table."""
    __tablename__ = 'empresa'
    
    cnpj_basico = Column(Text, nullable=False, primary_key=True, comment="CNPJ base (8 first digits).")
    razao_social = Column(Text, nullable=True, comment="Business name of the company.")
    natureza_juridica = Column(Text, nullable=True, comment="Legal nature code.")
    qualificacao_responsavel = Column(Text, nullable=True, comment="Qualification code of the responsible person.")
    capital_social = Column(Float(53), nullable=True, comment="Company's share capital.")
    porte_empresa = Column(Text, nullable=True, comment="Company size code (e.g., MEI, EPP).")
    ente_federativo_responsavel = Column(Text, nullable=True, comment="Responsible federative entity (for public companies).")
    
    __table_args__ = (
        Index('empresa_cnpj_basico_idx', 'cnpj_basico'), # Changed name for clarity if possible
    )


class SimplesNacional(Base):
    """SQLAlchemy model for the 'simples' (Simples Nacional tax regime) table."""
    __tablename__ = 'simples'
    
    cnpj_basico = Column(Text, nullable=True, primary_key=True, comment="CNPJ base (8 first digits).")
    opcao_pelo_simples = Column(Text, nullable=True, comment="Indicator if company opted for Simples Nacional (S/N).")
    data_opcao_simples = Column(Text, nullable=True, comment="Date of opting for Simples Nacional.")
    data_exclusao_simples = Column(Text, nullable=True, comment="Date of exclusion from Simples Nacional.")
    opcao_mei = Column(Text, nullable=True, comment="Indicator if company opted for MEI (Microempreendedor Individual) (S/N).")
    data_opcao_mei = Column(Text, nullable=True, comment="Date of opting for MEI.")
    data_exclusao_mei = Column(Text, nullable=True, comment="Date of exclusion from MEI.")
    
    __table_args__ = (
        Index('simples_cnpj_basico_idx', 'cnpj_basico'),
    )

class Socios(Base):
    """SQLAlchemy model for the 'socios' (partners/shareholders) table."""
    __tablename__ = 'socios'
    
    # Assuming cnpj_basico alone might not be unique for socios, often a composite PK is needed or an auto-increment ID.
    # For this dataset, it seems multiple partners can belong to one cnpj_basico.
    # The original schema has only cnpj_basico as PK, which means one row per cnpj_basico. This needs to be verified against data semantics.
    # If one company (cnpj_basico) can have multiple partners, this PK is wrong.
    # For now, I'll keep it as defined if it's from an existing schema, but add a comment.
    cnpj_basico = Column(Text, nullable=False, primary_key=True, comment="CNPJ base (8 first digits). If a company has multiple partners, this PK might be problematic.")
    identificador_socio = Column(Text, nullable=True, comment="Partner identifier type (e.g., individual, company).")
    nome_socio_razao_social = Column(Text, nullable=True, comment="Partner's name (individual) or business name (company).")
    cpf_cnpj_socio = Column(Text, nullable=True, comment="Partner's CPF (individual) or CNPJ (company).")
    qualificacao_socio = Column(Text, nullable=True, comment="Partner's qualification code.")
    data_entrada_sociedade = Column(Text, nullable=True, comment="Date when partner joined the company.")
    pais = Column(Text, nullable=True, comment="Country code of the partner (if foreign).")
    representante_legal = Column(Text, nullable=True, comment="CPF of the legal representative (if any).")
    nome_do_representante = Column(Text, nullable=True, comment="Name of the legal representative.")
    qualificacao_representante_legal = Column(Text, nullable=True, comment="Qualification code of the legal representative.")
    faixa_etaria = Column(Text, nullable=True, comment="Age group of the partner.")
    
    __table_args__ = (
        Index('socios_cnpj_basico_idx', 'cnpj_basico'),
        # Consider an index on cpf_cnpj_socio if lookups are frequent on this field
        # Index('socios_cpf_cnpj_socio_idx', 'cpf_cnpj_socio'),
    )

class Estabelecimento(Base):
    """SQLAlchemy model for the 'estabelecimento' (business establishment/branch) table."""
    __tablename__ = 'estabelecimento'
    
    # Composite primary key
    cnpj_basico = Column(Text, nullable=False, primary_key=True, comment="CNPJ base (8 first digits). Part of composite PK.")
    cnpj_ordem = Column(Text, nullable=True, primary_key=True, comment="CNPJ order number (4 digits). Part of composite PK.")
    cnpj_dv = Column(Text, nullable=True, primary_key=True, comment="CNPJ verification digit (2 digits). Part of composite PK.")

    identificador_matriz_filial = Column(Text, nullable=True, comment="Identifier for head office (1) or branch (2).")
    nome_fantasia = Column(Text, nullable=True, comment="Trade name/fantasy name.")
    situacao_cadastral = Column(Text, nullable=True, comment="Registration status code.")
    data_situacao_cadastral = Column(Text, nullable=True, comment="Date of current registration status.")
    motivo_situacao_cadastral = Column(Text, nullable=True, comment="Code for reason of registration status.")
    nome_cidade_exterior = Column(Text, nullable=True, comment="Name of city abroad (if applicable).")
    pais = Column(Text, nullable=True, comment="Country code (if applicable).")
    data_inicio_atividade = Column(Text, nullable=True, comment="Date of start of activities.")
    cnae_fiscal_principal = Column(Text, nullable=True, comment="Primary CNAE (National Classification of Economic Activities) code.")
    cnae_fiscal_secundaria = Column(Text, nullable=True, comment="Secondary CNAE codes (comma-separated string or JSON).") # Check data format
    tipo_logradouro = Column(Text, nullable=True, comment="Address type (e.g., Rua, Avenida).")
    logradouro = Column(Text, nullable=True, comment="Address street name.")
    numero = Column(Text, nullable=True, comment="Address number.")
    complemento = Column(Text, nullable=True, comment="Address complement (e.g., Apt, Suite).")
    bairro = Column(Text, nullable=True, comment="Address neighborhood/district.")
    cep = Column(Text, nullable=True, comment="Address ZIP code.")
    uf = Column(Text, nullable=True, comment="Address state (Federative Unit).")
    municipio = Column(Text, nullable=True, comment="Address municipality code.")
    ddd_1 = Column(Text, nullable=True, comment="Primary phone DDD.")
    telefone_1 = Column(Text, nullable=True, comment="Primary phone number.")
    ddd_2 = Column(Text, nullable=True, comment="Secondary phone DDD.")
    telefone_2 = Column(Text, nullable=True, comment="Secondary phone number.")
    ddd_fax = Column(Text, nullable=True, comment="Fax DDD.")
    fax = Column(Text, nullable=True, comment="Fax number.")
    correio_eletronico = Column(Text, nullable=True, comment="Email address.")
    situacao_especial = Column(Text, nullable=True, comment="Special situation status.")
    data_situacao_especial = Column(Text, nullable=True, comment="Date of special situation status.")
    
    __table_args__ = (
        Index('estabelecimento_cnpj_basico_idx', 'cnpj_basico'),
        Index('estabelecimento_cnpj_ordem_idx', 'cnpj_ordem'),
        Index('estabelecimento_cnpj_dv_idx', 'cnpj_dv'),
        Index('estabelecimento_cnae_principal_idx', 'cnae_fiscal_principal'),
        # Index('estabelecimento_cnae_secundaria_idx', 'cnae_fiscal_secundaria'), # Indexing this might be complex if it's a list in a string
        Index('estabelecimento_cep_idx', 'cep'),
        Index('estabelecimento_uf_idx', 'uf'),
        Index('estabelecimento_municipio_idx', 'municipio'),
    )

class Qualificacoes(Base):
    """SQLAlchemy model for 'quals' (partner qualifications) lookup table."""
    __tablename__ = 'quals'
    
    codigo = Column(Text, nullable=True, primary_key=True, comment="Qualification code.")
    descricao = Column(Text, nullable=True, comment="Description of the qualification.")

class MotivoCadastral(Base):
    """SQLAlchemy model for 'moti' (registration status reasons) lookup table."""
    __tablename__ = 'moti'
    
    codigo = Column(Text, nullable=True, primary_key=True, comment="Reason code.")
    descricao = Column(Text, nullable=True, comment="Description of the reason.")

class NaturezaJuridica(Base):
    """SQLAlchemy model for 'natju' (legal natures) lookup table."""
    __tablename__ = 'natju'
    
    codigo = Column(Text, nullable=True, primary_key=True, comment="Legal nature code.")
    descricao = Column(Text, nullable=True, comment="Description of the legal nature.")

class Municipio(Base):
    """SQLAlchemy model for 'munic' (municipalities) lookup table."""
    __tablename__ = 'munic'
    
    codigo = Column(Text, nullable=True, primary_key=True, comment="Municipality code (IBGE).")
    descricao = Column(Text, nullable=True, comment="Name of the municipality.")

class Cnae(Base):
    """SQLAlchemy model for 'cnae' (National Classification of Economic Activities) lookup table."""
    __tablename__ = 'cnae'
    
    codigo = Column(Text, nullable=True, primary_key=True, comment="CNAE code.")
    descricao = Column(Text, nullable=True, comment="Description of the CNAE activity.")

class Pais(Base):
    """SQLAlchemy model for 'pais' (countries) lookup table."""
    __tablename__ = 'pais'
    
    codigo = Column(Text, nullable=True, primary_key=True, comment="Country code.")
    descricao = Column(Text, nullable=True, comment="Name of the country.")