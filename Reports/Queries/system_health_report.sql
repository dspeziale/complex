-- System Health Report
-- Report salute sistema con metriche chiave
SELECT 
    'Database Connections' as metric_category,
    COUNT(*) as current_value,
    'Active connections to database' as description,
    GETDATE() as snapshot_time
FROM sys.dm_exec_connections

UNION ALL

SELECT 
    'Error Rate',
    COUNT(*),
    'Errors in last 24 hours',
    GETDATE()
FROM error_logs 
WHERE created_date >= DATEADD(day, -1, GETDATE())

UNION ALL

SELECT 
    'Active Users',
    COUNT(DISTINCT user_id),
    'Users active in last hour',
    GETDATE()
FROM user_activity 
WHERE activity_time >= DATEADD(hour, -1, GETDATE())

UNION ALL

SELECT 
    'Pending Jobs',
    COUNT(*),
    'Jobs waiting for execution',
    GETDATE()
FROM job_queue 
WHERE status = 'PENDING';