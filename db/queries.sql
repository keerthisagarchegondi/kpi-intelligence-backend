-- ============================================
-- KPI Intelligence Backend - Optimized Queries
-- ============================================
-- Description: Production-optimized SQL queries for KPI Intelligence system
-- Database: PostgreSQL
-- Performance: All queries optimized with proper indexing and execution plans
-- ============================================

-- ============================================
-- DATA SOURCES QUERIES
-- ============================================

-- Query 1: Get all active data sources (optimized with index on is_enabled and status)
-- Index used: idx_data_sources_is_enabled, idx_data_sources_status
-- Performance: O(log n) lookup
SELECT 
    id,
    name,
    description,
    source_type,
    status,
    last_sync_time,
    next_sync_time,
    created_at,
    updated_at
FROM data_sources
WHERE is_enabled = true 
    AND status = 'active'
    AND is_deleted = false
ORDER BY name;

-- Query 2: Get data sources by type with connection status (optimized)
-- Index used: idx_data_sources_source_type, idx_data_sources_is_deleted
-- Performance: Filtered scan with index
SELECT 
    id,
    name,
    source_type,
    status,
    last_connection_status,
    last_connection_test,
    last_error_message,
    is_enabled
FROM data_sources
WHERE source_type = $1
    AND is_deleted = false
ORDER BY last_connection_test DESC NULLS LAST;

-- Query 3: Get data sources requiring sync (optimized with composite condition)
-- Index used: idx_data_sources_is_enabled, idx_data_sources_last_sync
-- Performance: Multi-index scan
SELECT 
    id,
    name,
    source_type,
    last_sync_time,
    next_sync_time,
    refresh_schedule
FROM data_sources
WHERE is_enabled = true
    AND status = 'active'
    AND is_deleted = false
    AND (
        next_sync_time IS NULL 
        OR next_sync_time <= CURRENT_TIMESTAMP
    )
ORDER BY COALESCE(next_sync_time, '1970-01-01'::timestamp) ASC;

-- Query 4: Get data source by name (unique index lookup)
-- Index used: idx_data_sources_name (unique)
-- Performance: O(1) direct lookup
SELECT 
    id,
    name,
    description,
    source_type,
    connection_config,
    auth_type,
    status,
    is_enabled,
    last_sync_time,
    next_sync_time,
    refresh_schedule,
    tags,
    metadata,
    created_at,
    updated_at
FROM data_sources
WHERE name = $1
    AND is_deleted = false;

-- Query 5: Search data sources by tags (optimized GIN index)
-- Index used: idx_data_sources_tags (GIN index on JSONB)
-- Performance: Fast JSONB containment check
SELECT 
    id,
    name,
    source_type,
    tags,
    status,
    created_at
FROM data_sources
WHERE tags @> $1::jsonb
    AND is_deleted = false
ORDER BY created_at DESC;

-- Query 6: Get data sources with failed connections (error monitoring)
-- Index used: idx_data_sources_status, idx_data_sources_is_enabled
-- Performance: Indexed scan
SELECT 
    id,
    name,
    source_type,
    status,
    last_connection_status,
    last_connection_test,
    last_error_message,
    last_sync_time
FROM data_sources
WHERE last_connection_status = 'failed'
    AND is_enabled = true
    AND is_deleted = false
ORDER BY last_connection_test DESC;

-- Query 7: Get data source statistics (aggregated metrics)
-- Performance: Full table scan with aggregation (cached for reporting)
SELECT 
    source_type,
    COUNT(*) as total_sources,
    COUNT(*) FILTER (WHERE status = 'active') as active_count,
    COUNT(*) FILTER (WHERE status = 'inactive') as inactive_count,
    COUNT(*) FILTER (WHERE status = 'error') as error_count,
    COUNT(*) FILTER (WHERE is_enabled = true) as enabled_count,
    COUNT(*) FILTER (WHERE last_connection_status = 'success') as healthy_count,
    COUNT(*) FILTER (WHERE last_connection_status = 'failed') as failed_count
FROM data_sources
WHERE is_deleted = false
GROUP BY source_type
ORDER BY total_sources DESC;

-- Query 8: Get recently added data sources (optimized with index)
-- Index used: idx_data_sources_created_at
-- Performance: Index-only scan with LIMIT
SELECT 
    id,
    name,
    source_type,
    status,
    created_at,
    created_by
FROM data_sources
WHERE is_deleted = false
ORDER BY created_at DESC
LIMIT $1;

-- Query 9: Update data source sync time (optimized update)
-- Index used: Primary key (id)
-- Performance: Direct key lookup, O(1)
UPDATE data_sources
SET 
    last_sync_time = CURRENT_TIMESTAMP,
    next_sync_time = CURRENT_TIMESTAMP + INTERVAL '1 hour',
    updated_at = CURRENT_TIMESTAMP,
    updated_by = $2
WHERE id = $1
    AND is_deleted = false
RETURNING id, name, last_sync_time, next_sync_time;

-- Query 10: Update connection status (health check update)
-- Index used: Primary key (id)
-- Performance: Direct key lookup with conditional update
UPDATE data_sources
SET 
    last_connection_status = $2,
    last_connection_test = CURRENT_TIMESTAMP,
    last_error_message = CASE 
        WHEN $2 = 'failed' THEN $3 
        ELSE NULL 
    END,
    status = CASE 
        WHEN $2 = 'failed' THEN 'error'
        WHEN $2 = 'success' AND status = 'error' THEN 'active'
        ELSE status
    END,
    updated_at = CURRENT_TIMESTAMP
WHERE id = $1
    AND is_deleted = false
RETURNING id, name, status, last_connection_status;

-- Query 11: Soft delete data source (maintains audit trail)
-- Index used: Primary key (id)
-- Performance: Direct key lookup
UPDATE data_sources
SET 
    is_deleted = true,
    deleted_at = CURRENT_TIMESTAMP,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = $2
WHERE id = $1
    AND is_deleted = false
RETURNING id, name;

-- Query 12: Bulk enable/disable data sources (batch operation)
-- Index used: idx_data_sources_source_type
-- Performance: Filtered update with index
UPDATE data_sources
SET 
    is_enabled = $1,
    updated_at = CURRENT_TIMESTAMP,
    updated_by = $2
WHERE source_type = $3
    AND is_deleted = false
RETURNING id, name, is_enabled;

-- ============================================
-- ADVANCED QUERIES - ANALYTICS & REPORTING
-- ============================================

-- Query 13: Data source health dashboard (complex aggregation)
-- Performance: Optimized aggregation with CTEs
WITH sync_metrics AS (
    SELECT 
        id,
        name,
        source_type,
        EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - last_sync_time))/3600 as hours_since_sync,
        CASE 
            WHEN last_sync_time IS NULL THEN 'never_synced'
            WHEN CURRENT_TIMESTAMP - last_sync_time < INTERVAL '1 hour' THEN 'recent'
            WHEN CURRENT_TIMESTAMP - last_sync_time < INTERVAL '24 hours' THEN 'stale'
            ELSE 'very_stale'
        END as sync_status
    FROM data_sources
    WHERE is_deleted = false AND is_enabled = true
)
SELECT 
    source_type,
    COUNT(*) as total_sources,
    COUNT(*) FILTER (WHERE sync_status = 'recent') as recently_synced,
    COUNT(*) FILTER (WHERE sync_status = 'stale') as stale,
    COUNT(*) FILTER (WHERE sync_status = 'very_stale') as very_stale,
    COUNT(*) FILTER (WHERE sync_status = 'never_synced') as never_synced,
    ROUND(AVG(hours_since_sync)::numeric, 2) as avg_hours_since_sync
FROM sync_metrics
GROUP BY source_type
ORDER BY total_sources DESC;

-- Query 14: Get data source with metadata search (JSONB query optimization)
-- Index used: idx_data_sources_metadata (GIN index)
-- Performance: Fast JSONB path query
SELECT 
    id,
    name,
    source_type,
    metadata,
    created_at
FROM data_sources
WHERE metadata @> $1::jsonb
    AND is_deleted = false
ORDER BY created_at DESC;

-- Query 15: Active data sources with pagination (optimized for large datasets)
-- Index used: idx_data_sources_is_deleted, idx_data_sources_created_at
-- Performance: Keyset pagination for better performance than OFFSET
SELECT 
    id,
    name,
    source_type,
    status,
    last_sync_time,
    created_at
FROM data_sources
WHERE is_deleted = false
    AND is_enabled = true
    AND ($1::INTEGER IS NULL OR id > $1)
ORDER BY id
LIMIT $2;

-- Query 16: Data source uptime statistics (reliability metrics)
-- Performance: Window function with filtering
WITH connection_tests AS (
    SELECT 
        id,
        name,
        source_type,
        last_connection_status,
        last_connection_test,
        LAG(last_connection_test) OVER (PARTITION BY id ORDER BY last_connection_test) as prev_test
    FROM data_sources
    WHERE is_deleted = false
        AND last_connection_test IS NOT NULL
)
SELECT 
    id,
    name,
    source_type,
    last_connection_status,
    last_connection_test,
    ROUND(
        EXTRACT(EPOCH FROM (last_connection_test - COALESCE(prev_test, last_connection_test)))::numeric / 3600,
        2
    ) as hours_between_tests
FROM connection_tests
WHERE prev_test IS NOT NULL
ORDER BY last_connection_test DESC;

-- ============================================
-- MATERIALIZED VIEW FOR PERFORMANCE
-- ============================================

-- Create materialized view for data source summary (refresh periodically)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_data_source_summary AS
SELECT 
    id,
    name,
    source_type,
    status,
    is_enabled,
    last_sync_time,
    last_connection_status,
    CURRENT_TIMESTAMP - last_sync_time as time_since_sync,
    CASE 
        WHEN status = 'active' AND last_connection_status = 'success' THEN 'healthy'
        WHEN status = 'error' OR last_connection_status = 'failed' THEN 'unhealthy'
        WHEN status = 'inactive' THEN 'disabled'
        ELSE 'unknown'
    END as health_status,
    created_at,
    updated_at
FROM data_sources
WHERE is_deleted = false;

-- Create unique index on materialized view
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_data_source_summary_id 
ON mv_data_source_summary(id);

CREATE INDEX IF NOT EXISTS idx_mv_data_source_summary_health 
ON mv_data_source_summary(health_status);

-- Query 17: Fast summary query using materialized view
-- Performance: Direct materialized view read (very fast)
SELECT 
    source_type,
    COUNT(*) as total,
    COUNT(*) FILTER (WHERE health_status = 'healthy') as healthy,
    COUNT(*) FILTER (WHERE health_status = 'unhealthy') as unhealthy,
    COUNT(*) FILTER (WHERE health_status = 'disabled') as disabled
FROM mv_data_source_summary
GROUP BY source_type;

-- ============================================
-- QUERY OPTIMIZATION NOTES
-- ============================================
-- 
-- 1. All queries use existing indexes for optimal performance
-- 2. Parameterized queries ($1, $2, etc.) prevent SQL injection
-- 3. JSONB queries use GIN indexes for fast containment checks
-- 4. Soft deletes (is_deleted) maintain data integrity
-- 5. Window functions used efficiently for analytics
-- 6. CTEs improve readability without performance penalty
-- 7. Materialized views for expensive aggregations
-- 8. Keyset pagination (id > $1) instead of OFFSET for large datasets
-- 9. COALESCE and NULLS LAST for proper NULL handling
-- 10. FILTER clause for cleaner aggregations
--
-- Performance Tips:
-- - Refresh materialized view periodically: REFRESH MATERIALIZED VIEW CONCURRENTLY mv_data_source_summary;
-- - Use EXPLAIN ANALYZE to verify query plans
-- - Monitor slow query log for optimization opportunities
-- - Consider partitioning for very large tables (> 10M rows)
-- - Use connection pooling for high-concurrency scenarios
-- ============================================
