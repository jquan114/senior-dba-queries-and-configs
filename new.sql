-- 1. Performance Analysis of Top Queries

SELECT TOP 20 qs.total_logical_reads, qs.total_logical_writes, qs.execution_count,
       qs.total_elapsed_time, qs.total_elapsed_time / qs.execution_count AS avg_elapsed_time,
       SUBSTRING(qt.text, qs.statement_start_offset / 2 + 1, 
                 (CASE WHEN qs.statement_end_offset = -1 
                       THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2 
                       ELSE qs.statement_end_offset 
                  END - qs.statement_start_offset) / 2) AS query_text
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
ORDER BY qs.total_logical_reads DESC;

-- Purpose: Identify the top 20 most resource-intensive queries for performance tuning.



-------------------

 -- 2. Index Usage Analysis
SELECT o.name, i.name, i.index_id, dm_ius.user_seeks, dm_ius.user_scans,
       dm_ius.user_lookups, dm_ius.user_updates
FROM sys.dm_db_index_usage_stats AS dm_ius
INNER JOIN sys.indexes AS i ON i.index_id = dm_ius.index_id AND i.object_id = dm_ius.object_id
INNER JOIN sys.objects AS o ON i.object_id = o.object_id
WHERE dm_ius.database_id = DB_ID()
ORDER BY dm_ius.user_seeks + dm_ius.user_scans + dm_ius.user_lookups + dm_ius.user_updates DESC;

-- Purpose: Evaluate which indexes are being used the most and which ones might be candidates for optimization.

-------------------
 -- 3. Database Size Monitoring
SELECT database_name = DB_NAME(database_id), 
       log_size_mb = CAST(SUM(CASE WHEN type_desc = 'LOG' THEN size END) * 8 / 1024 AS DECIMAL(10, 2)),
       row_size_mb = CAST(SUM(CASE WHEN type_desc = 'ROWS' THEN size END) * 8 / 1024 AS DECIMAL(10, 2))
FROM sys.master_files
GROUP BY database_id;

-- Purpose: Monitor the size of each database, separating data and log file sizes.

-- 4. Identifying Long Running Queries

SELECT r.session_id, r.start_time, r.status, r.command, 
       s.text AS sql_text, 
       r.wait_type, r.wait_time, r.cpu_time, r.logical_reads
FROM sys.dm_exec_requests r
CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) s
WHERE r.status = 'running'
AND r.start_time < DATEADD(MINUTE, -5, GETDATE());
-- Purpose: Detect queries that have been running for more than 5 minutes.

-- 5. Backup Status Check

SELECT database_name, backup_start_date, backup_finish_date, 
       DATEDIFF(minute, backup_start_date, backup_finish_date) AS backup_duration_minutes,
       backup_size / 1024 / 1024 AS backup_size_mb, 
       CASE WHEN is_copy_only = 1 THEN 'Copy Only' ELSE 'Full' END AS backup_type
FROM msdb.dbo.backupset
WHERE backup_start_date > DATEADD(day, -7, GETDATE())
ORDER BY backup_start_date DESC;
-- Purpose: Review the backup history over the last week.

-- 6. Database File Growth Events

SELECT DB_NAME(database_id) AS database_name, file_id, number_of_pages, 
       growth, growth * 8 / 1024 AS growth_mb, timestamp
FROM sys.master_files
CROSS APPLY sys.fn_dblog(NULL, NULL) 
WHERE operation = 'LOP_MODIFY_ROW' AND context = 'LCX_MARK_AS_GHOST'
ORDER BY timestamp DESC;
-- Purpose: Track file growth events for capacity planning.
-- 7. Detecting Database Corruption

DBCC CHECKDB WITH NO_INFOMSGS, ALL_ERRORMSGS;
-- Purpose: Run integrity checks on databases to detect corruption.
-- 8. Monitoring Log Shipping Status

SELECT ls.primary_database, ls.backup_source_directory, 
       ls.backup_destination_directory, 
       ls.last_backup_file, ls.last_backup_date
FROM msdb.dbo.log_shipping_monitor_primary AS ls
WHERE ls.last_backup_date < DATEADD(hour, -1, GETDATE());
-- Purpose: Ensure log shipping is occurring as expected and identify any lags.

-- 9. Security Audit: User Access Levels

SELECT p.name AS principal_name, p.type_desc AS principal_type, 
       p.authentication_type_desc, 
       dp.permission_name, dp.state_desc AS permission_state
FROM sys.database_principals p
LEFT JOIN sys.database_permissions dp ON p.principal_id = dp.grantee_principal_id
ORDER BY p.name, dp.permission_name;
-- Purpose: Review user access levels and permissions for security audits.

-- 10. Querying Deadlocks

SELECT XEvent.query('(event/data/value)[1]') AS deadlock_graph
FROM (
    SELECT CAST(target_data AS XML) AS TargetData
    FROM sys.dm_xe_session_targets st
    JOIN sys.dm_xe_sessions s ON s.address = st.event_session_address
    WHERE name = 'system_health'
    AND target_name = 'ring_buffer'
) AS Data
CROSS APPLY TargetData.nodes('RingBufferTarget/event[@name="xml_deadlock_report"]') AS XEvent (event);
-- Purpose: Extract deadlock information from the system health session.

11. Failed Login Attempts
-- sql

SELECT event_time, action_id, succeeded, server_principal_name, database_name, client_ip
FROM sys.fn_get_audit_file ('path_to_audit_file', default, default)
WHERE action_id = 'LGIF';
-- Purpose: Review failed login attempts for security monitoring.

-- 12. SQL Agent Job Failures

SELECT j.name AS job_name, jh.run_date, jh.run_time, jh.message
FROM msdb.dbo.sysjobs j
JOIN msdb.dbo.sysjobhistory jh ON j.job_id = jh.job_id
WHERE jh.run_status = 0
ORDER BY jh.run_date DESC, jh.run_time DESC;
Purpose: Check for any SQL Agent job failures.

-- 13. Database Mirroring Status

SELECT DB_NAME(database_id) AS database_name, mirroring_state_desc, 
       mirroring_role_desc, mirroring_partner_name, mirroring_witness_name
FROM sys.database_mirroring
WHERE mirroring_guid IS NOT NULL;
Purpose: Monitor the status of database mirroring setups.

-- 14. Always On Availability Groups Health Check

SELECT ag.name AS ag_name, replica_server_name, db_name(database_id) AS database_name, 
       synchronization_state_desc, synchronization_health_desc
FROM sys.dm_hadr_database_replica_states drs
INNER JOIN sys.availability_groups ag ON drs.group_id = ag.group_id
ORDER BY ag.name, replica_server_name, database_name;
Purpose: Check the health of Always On Availability Groups.

-- 15. Database Collation Check

SELECT name, collation_name
FROM sys.databases
WHERE collation_name <> SERVERPROPERTY('Collation');
Purpose: Ensure consistency in database collations with the server collation.

-- 16. Find Implicit Conversions in Queries

SELECT DISTINCT TOP 50 qs.plan_handle, qs.query_hash, 
       SUBSTRING(qt.text, qs.statement_start_offset / 2, 
       (CASE WHEN qs.statement_end_offset = -1 
             THEN LEN(CONVERT(nvarchar(max), qt.text)) * 2 
             ELSE qs.statement_end_offset 
        END - qs.statement_start_offset) / 2) AS query_text
FROM sys.dm_exec_query_stats AS qs
CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS qt
WHERE qt.text LIKE '%CONVERT(%' OR qt.text LIKE '%CAST(%';
-- Purpose: Identify potential performance issues due to implicit conversions in queries.

-- 17. Database Snapshot Creation

CREATE DATABASE [DB_Snapshot] ON 
(NAME = N'DB_Data', FILENAME = N'path_to_snapshot_file')
AS SNAPSHOT OF [DB_Name];
-- Purpose: Create a database snapshot for a point-in-time view of a database.

-- 18. Resource Governor Configuration

SELECT configuration_id, name, value, minimum, maximum, value_in_use, is_dynamic, is_advanced
FROM sys.configurations
WHERE name LIKE '%resource governor%';
Purpose: Review Resource Governor settings for managing SQL Server workloads.

-- 19. Track TempDB Usage

SELECT r.session_id, r.request_id, t.name AS table_name, 
       SUM(user_object_reserved_page_count) * 8 AS user_objects_kb
FROM sys.dm_db_session_space_usage AS s
INNER JOIN sys.dm_db_task_space_usage AS t ON s.session_id = t.session_id
INNER JOIN tempdb.sys.tables AS r ON t.internal_objects_alloc_page_count = r.object_id
GROUP BY r.session_id, r.request_id, t.name;

