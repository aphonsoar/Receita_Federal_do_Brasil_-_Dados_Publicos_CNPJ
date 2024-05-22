CREATE TABLE audit_file_uploads (
    audi_id UUID PRIMARY KEY,                 -- Unique identifier (auto-increment)
    audi_filestem VARCHAR(255) NOT NULL,      -- Filename without extension
    audi_downloaded_at DATETIME NOT NULL,     -- Date and time of file download
    audi_processed_at DATETIME DEFAULT NULL   -- Date and time of file processing (optional)
);