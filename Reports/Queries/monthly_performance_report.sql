-- Monthly Performance Report
-- Report prestazioni mensili con trend
WITH monthly_stats AS (
    SELECT 
        YEAR(created_date) as report_year,
        MONTH(created_date) as report_month,
        COUNT(*) as total_records,
        COUNT(DISTINCT user_id) as unique_users,
        AVG(processing_time_ms) as avg_processing_time,
        SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) as successful_operations,
        SUM(CASE WHEN status = 'ERROR' THEN 1 ELSE 0 END) as failed_operations
    FROM operation_logs
    WHERE created_date >= DATEADD(month, -12, GETDATE())
    GROUP BY YEAR(created_date), MONTH(created_date)
)
SELECT 
    report_year,
    report_month,
    DATENAME(month, DATEFROMPARTS(report_year, report_month, 1)) as month_name,
    total_records,
    unique_users,
    avg_processing_time,
    successful_operations,
    failed_operations,
    CASE 
        WHEN total_records > 0 
        THEN CAST(successful_operations * 100.0 / total_records AS DECIMAL(5,2))
        ELSE 0 
    END as success_rate_percent
FROM monthly_stats
ORDER BY report_year DESC, report_month DESC;