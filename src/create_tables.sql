CREATE TABLE audit (
    audit_id SERIAL PRIMARY KEY,                    -- Use SERIAL for auto-incrementing integer ID
    audi_filename VARCHAR(255) NOT NULL,            -- Filename without extension
    audi_source_updated_at TIMESTAMP DEFAULT NULL,  -- Date and time of source file update 
    audi_created_at  TIMESTAMP DEFAULT NULL,        -- 
    audi_processed_at TIMESTAMP DEFAULT NULL,       -- Date and time of file processing
    audi_downloaded_at TIMESTAMP DEFAULT NULL,      -- Date and time of file download  
    audi_inserted_at TIMESTAMP DEFAULT NULL         -- Date and time of file insertion
);

