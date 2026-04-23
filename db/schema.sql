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
