-- ============================================
-- KPI Intelligence Backend - Database Schema
-- ============================================
-- Description: Core database schema for KPI Intelligence system
-- Created: 2026-04-23
-- Database: PostgreSQL
-- ============================================

-- Drop tables if they exist (in reverse order of dependencies)
DROP TABLE IF EXISTS data_sources CASCADE;

-- ============================================
-- Table: data_sources
-- Description: Stores configuration and metadata for various data sources
-- ============================================
CREATE TABLE data_sources (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Basic information
    name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    
    -- Data source type (e.g., 'database', 'api', 'file', 'cloud_storage')
    source_type VARCHAR(50) NOT NULL,
    
    -- Connection configuration (stored as JSON)
    connection_config JSONB NOT NULL,
    
    -- Authentication details (encrypted/hashed in production)
    auth_type VARCHAR(50),  -- e.g., 'basic', 'oauth', 'api_key', 'certificate'
    auth_credentials JSONB, -- Encrypted credentials
    
    -- Status and health
    status VARCHAR(20) DEFAULT 'active' CHECK (status IN ('active', 'inactive', 'error', 'testing')),
    is_enabled BOOLEAN DEFAULT true,
    last_connection_test TIMESTAMP,
    last_connection_status VARCHAR(20), -- 'success' or 'failed'
    last_error_message TEXT,
    
    -- Data refresh configuration
    refresh_schedule VARCHAR(100), -- Cron expression or schedule description
    last_sync_time TIMESTAMP,
    next_sync_time TIMESTAMP,
    
    -- Metadata
    tags JSONB, -- Array of tags for categorization
    metadata JSONB, -- Additional flexible metadata
    
    -- Audit fields
    created_by VARCHAR(255),
    updated_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Soft delete
    deleted_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT false
);

-- ============================================
-- Indexes for performance optimization
-- ============================================
CREATE INDEX idx_data_sources_name ON data_sources(name);
CREATE INDEX idx_data_sources_source_type ON data_sources(source_type);
CREATE INDEX idx_data_sources_status ON data_sources(status);
CREATE INDEX idx_data_sources_is_enabled ON data_sources(is_enabled);
CREATE INDEX idx_data_sources_is_deleted ON data_sources(is_deleted);
CREATE INDEX idx_data_sources_created_at ON data_sources(created_at);
CREATE INDEX idx_data_sources_last_sync ON data_sources(last_sync_time);

-- GIN index for JSONB columns (faster queries on JSON data)
CREATE INDEX idx_data_sources_connection_config ON data_sources USING GIN(connection_config);
CREATE INDEX idx_data_sources_tags ON data_sources USING GIN(tags);
CREATE INDEX idx_data_sources_metadata ON data_sources USING GIN(metadata);

-- ============================================
-- Triggers
-- ============================================
-- Trigger to automatically update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER update_data_sources_updated_at
    BEFORE UPDATE ON data_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Comments for documentation
-- ============================================
COMMENT ON TABLE data_sources IS 'Stores configuration and metadata for all data sources used in the KPI Intelligence system';
COMMENT ON COLUMN data_sources.id IS 'Unique identifier for the data source';
COMMENT ON COLUMN data_sources.name IS 'Human-readable name of the data source (must be unique)';
COMMENT ON COLUMN data_sources.source_type IS 'Type of data source: database, api, file, cloud_storage, etc.';

-- ============================================
-- Table: datasets
-- Description: Stores information about ingested datasets and their processing status
-- ============================================
DROP TABLE IF EXISTS datasets CASCADE;

CREATE TABLE datasets (
    -- Primary key
    id SERIAL PRIMARY KEY,
    
    -- Foreign key to data source
    data_source_id INTEGER REFERENCES data_sources(id) ON DELETE SET NULL,
    
    -- Dataset identification
    name VARCHAR(255) NOT NULL,
    display_name VARCHAR(255),
    description TEXT,
    dataset_type VARCHAR(50) NOT NULL, -- e.g., 'sales', 'financial', 'operational', 'customer'
    
    -- File/Data information
    file_name VARCHAR(255),
    file_path TEXT,
    file_format VARCHAR(50), -- e.g., 'csv', 'json', 'xlsx', 'parquet', 'xml'
    file_size_bytes BIGINT,
    file_hash VARCHAR(64), -- SHA-256 hash for integrity verification
    
    -- Data characteristics
    row_count BIGINT,
    column_count INTEGER,
    schema_info JSONB, -- Column names, types, and metadata
    data_quality_score DECIMAL(5,2), -- 0-100 quality score
    
    -- Processing status
    status VARCHAR(30) DEFAULT 'pending' CHECK (status IN (
        'pending',
        'validating',
        'processing',
        'completed',
        'failed',
        'archived'
    )),
    processing_stage VARCHAR(50), -- e.g., 'ingestion', 'transformation', 'validation'
    
    -- Processing metrics
    ingestion_start_time TIMESTAMP,
    ingestion_end_time TIMESTAMP,
    processing_duration_seconds INTEGER,
    records_processed BIGINT,
    records_failed BIGINT,
    records_skipped BIGINT,
    
    -- Error tracking
    error_count INTEGER DEFAULT 0,
    last_error_message TEXT,
    error_details JSONB, -- Structured error information
    validation_errors JSONB, -- Array of validation issues
    
    -- Data versioning
    version INTEGER DEFAULT 1,
    is_latest_version BOOLEAN DEFAULT true,
    parent_dataset_id INTEGER REFERENCES datasets(id) ON DELETE SET NULL,
    
    -- Data lineage and dependencies
    source_system VARCHAR(255),
    source_location TEXT,
    dependencies JSONB, -- Array of dependent dataset IDs or external references
    
    -- Data retention and lifecycle
    retention_policy VARCHAR(50), -- e.g., '90_days', '1_year', 'permanent'
    expiration_date DATE,
    archived_at TIMESTAMP,
    archive_location TEXT,
    
    -- Business metadata
    owner VARCHAR(255),
    business_unit VARCHAR(100),
    tags JSONB, -- Array of tags for categorization
    custom_metadata JSONB, -- Flexible additional metadata
    
    -- Data quality and validation
    is_validated BOOLEAN DEFAULT false,
    validated_at TIMESTAMP,
    validated_by VARCHAR(255),
    data_profile JSONB, -- Statistical profile of the dataset
    
    -- Access and usage tracking
    access_count INTEGER DEFAULT 0,
    last_accessed_at TIMESTAMP,
    last_accessed_by VARCHAR(255),
    
    -- Audit fields
    created_by VARCHAR(255) NOT NULL,
    updated_by VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL,
    
    -- Soft delete
    deleted_at TIMESTAMP,
    is_deleted BOOLEAN DEFAULT false,
    
    -- Constraints
    CONSTRAINT unique_dataset_name_version UNIQUE (name, version, is_deleted),
    CONSTRAINT valid_quality_score CHECK (data_quality_score IS NULL OR (data_quality_score >= 0 AND data_quality_score <= 100)),
    CONSTRAINT valid_duration CHECK (processing_duration_seconds IS NULL OR processing_duration_seconds >= 0),
    CONSTRAINT valid_counts CHECK (
        (row_count IS NULL OR row_count >= 0) AND
        (column_count IS NULL OR column_count >= 0) AND
        (records_processed IS NULL OR records_processed >= 0) AND
        (records_failed IS NULL OR records_failed >= 0) AND
        (records_skipped IS NULL OR records_skipped >= 0)
    )
);

-- ============================================
-- Indexes for datasets table
-- ============================================
CREATE INDEX idx_datasets_data_source_id ON datasets(data_source_id);
CREATE INDEX idx_datasets_name ON datasets(name);
CREATE INDEX idx_datasets_status ON datasets(status);
CREATE INDEX idx_datasets_dataset_type ON datasets(dataset_type);
CREATE INDEX idx_datasets_created_at ON datasets(created_at);
CREATE INDEX idx_datasets_ingestion_start ON datasets(ingestion_start_time);
CREATE INDEX idx_datasets_is_latest ON datasets(is_latest_version);
CREATE INDEX idx_datasets_is_deleted ON datasets(is_deleted);
CREATE INDEX idx_datasets_owner ON datasets(owner);
CREATE INDEX idx_datasets_business_unit ON datasets(business_unit);
CREATE INDEX idx_datasets_file_hash ON datasets(file_hash);
CREATE INDEX idx_datasets_parent ON datasets(parent_dataset_id);

-- Composite indexes for common query patterns
CREATE INDEX idx_datasets_status_created ON datasets(status, created_at DESC);
CREATE INDEX idx_datasets_type_status ON datasets(dataset_type, status);
CREATE INDEX idx_datasets_latest_active ON datasets(is_latest_version, is_deleted) WHERE is_latest_version = true AND is_deleted = false;

-- GIN indexes for JSONB columns
CREATE INDEX idx_datasets_schema_info ON datasets USING GIN(schema_info);
CREATE INDEX idx_datasets_tags ON datasets USING GIN(tags);
CREATE INDEX idx_datasets_custom_metadata ON datasets USING GIN(custom_metadata);
CREATE INDEX idx_datasets_error_details ON datasets USING GIN(error_details);
CREATE INDEX idx_datasets_data_profile ON datasets USING GIN(data_profile);

-- ============================================
-- Triggers for datasets table
-- ============================================
CREATE TRIGGER update_datasets_updated_at
    BEFORE UPDATE ON datasets
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- ============================================
-- Comments for datasets table
-- ============================================
COMMENT ON TABLE datasets IS 'Stores metadata and processing status for all ingested datasets in the KPI Intelligence system';
COMMENT ON COLUMN datasets.id IS 'Unique identifier for the dataset';
COMMENT ON COLUMN datasets.data_source_id IS 'Reference to the data source this dataset originated from';
COMMENT ON COLUMN datasets.name IS 'Unique technical name for the dataset';
COMMENT ON COLUMN datasets.display_name IS 'Human-readable display name';
COMMENT ON COLUMN datasets.dataset_type IS 'Classification of dataset type for categorization';
COMMENT ON COLUMN datasets.file_hash IS 'SHA-256 hash for file integrity verification and duplicate detection';
COMMENT ON COLUMN datasets.schema_info IS 'JSONB structure containing column definitions and data types';
COMMENT ON COLUMN datasets.status IS 'Current processing status of the dataset';
COMMENT ON COLUMN datasets.version IS 'Version number for dataset versioning support';
COMMENT ON COLUMN datasets.is_latest_version IS 'Flag indicating if this is the most recent version';
COMMENT ON COLUMN datasets.data_quality_score IS 'Calculated quality score from 0-100 based on validation rules';
COMMENT ON COLUMN datasets.retention_policy IS 'Data retention policy defining how long to keep the dataset';
COMMENT ON COLUMN data_sources.connection_config IS 'JSON object containing connection parameters (host, port, database name, etc.)';
COMMENT ON COLUMN data_sources.auth_credentials IS 'Encrypted authentication credentials stored as JSON';
COMMENT ON COLUMN data_sources.status IS 'Current operational status of the data source';
COMMENT ON COLUMN data_sources.refresh_schedule IS 'Schedule for automatic data synchronization (cron format or description)';
COMMENT ON COLUMN data_sources.tags IS 'JSON array of tags for categorization and filtering';
COMMENT ON COLUMN data_sources.is_deleted IS 'Soft delete flag - true if record is logically deleted';

-- ============================================
-- Sample data for testing (optional)
-- ============================================
-- INSERT INTO data_sources (name, description, source_type, connection_config, status, is_enabled)
-- VALUES 
--     ('Production Database', 'Main production PostgreSQL database', 'database', 
--      '{"host": "localhost", "port": 5432, "database": "production_db"}', 'active', true),
--     ('Sales API', 'External sales data API', 'api', 
--      '{"endpoint": "https://api.example.com/sales", "version": "v1"}', 'active', true),
--     ('CSV Data Files', 'Local CSV file storage', 'file', 
--      '{"path": "/data/csv", "format": "csv"}', 'active', true);
