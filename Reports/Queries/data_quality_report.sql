-- Data Quality Report
-- Report qualità dati con controlli di integrità
WITH quality_checks AS (
    SELECT 
        'Missing Email Addresses' as check_name,
        COUNT(*) as issue_count,
        'users' as table_name,
        'email IS NULL OR email = ''' as condition_desc
    FROM users 
    WHERE email IS NULL OR email = ''

    UNION ALL

    SELECT 
        'Duplicate Records',
        COUNT(*) - COUNT(DISTINCT email),
        'users',
        'Duplicate email addresses'
    FROM users
    WHERE email IS NOT NULL

    UNION ALL

    SELECT 
        'Orphaned Records',
        COUNT(*),
        'user_profiles',
        'No matching user_id in users table'
    FROM user_profiles p
    WHERE NOT EXISTS (SELECT 1 FROM users u WHERE u.user_id = p.user_id)

    UNION ALL

    SELECT 
        'Invalid Dates',
        COUNT(*),
        'operations',
        'created_date in future or before 2020'
    FROM operations
    WHERE created_date > GETDATE() OR created_date < '2020-01-01'
)
SELECT 
    check_name,
    table_name,
    issue_count,
    condition_desc,
    CASE 
        WHEN issue_count = 0 THEN 'PASS'
        WHEN issue_count < 10 THEN 'WARNING'
        ELSE 'FAIL'
    END as status,
    GETDATE() as report_generated
FROM quality_checks
ORDER BY issue_count DESC;