-- User Activity Summary Report
-- Riepilogo attività utenti con statistiche aggregate
SELECT 
    u.user_id,
    u.username,
    u.full_name,
    u.department,
    COUNT(DISTINCT s.session_id) as total_sessions,
    MIN(s.login_time) as first_login,
    MAX(s.logout_time) as last_activity,
    AVG(DATEDIFF(minute, s.login_time, s.logout_time)) as avg_session_minutes,
    SUM(CASE WHEN s.login_time >= DATEADD(day, -30, GETDATE()) THEN 1 ELSE 0 END) as sessions_last_30_days
FROM users u
LEFT JOIN user_sessions s ON u.user_id = s.user_id
WHERE u.is_active = 1
GROUP BY u.user_id, u.username, u.full_name, u.department
ORDER BY last_activity DESC;