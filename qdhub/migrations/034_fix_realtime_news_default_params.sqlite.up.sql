-- 修复 realtime-news 计划：补全 default_execute_params 与 incremental_mode，使定时调度能执行
-- Version: 034
UPDATE sync_plan
SET default_execute_params = '{}',
    incremental_mode = 1
WHERE id = 'realtime-news'
  AND (default_execute_params IS NULL OR default_execute_params = '');
