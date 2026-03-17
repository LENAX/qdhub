-- 紧急修复：为 admin/operator/viewer 补全 api-sync-strategies 权限（解决 admin 无法修改 api-sync-strategy 的问题）
-- 在线上库用 sqlite3 执行: sqlite3 /path/to/qdhub.db < emergency_fix_casbin_api_sync_strategies.sql

-- admin: read, write, delete
INSERT INTO casbin_rule (ptype, v0, v1, v2) SELECT 'p','admin','api-sync-strategies','read' WHERE NOT EXISTS (SELECT 1 FROM casbin_rule WHERE ptype='p' AND v0='admin' AND v1='api-sync-strategies' AND v2='read');
INSERT INTO casbin_rule (ptype, v0, v1, v2) SELECT 'p','admin','api-sync-strategies','write' WHERE NOT EXISTS (SELECT 1 FROM casbin_rule WHERE ptype='p' AND v0='admin' AND v1='api-sync-strategies' AND v2='write');
INSERT INTO casbin_rule (ptype, v0, v1, v2) SELECT 'p','admin','api-sync-strategies','delete' WHERE NOT EXISTS (SELECT 1 FROM casbin_rule WHERE ptype='p' AND v0='admin' AND v1='api-sync-strategies' AND v2='delete');
-- operator: read, write
INSERT INTO casbin_rule (ptype, v0, v1, v2) SELECT 'p','operator','api-sync-strategies','read' WHERE NOT EXISTS (SELECT 1 FROM casbin_rule WHERE ptype='p' AND v0='operator' AND v1='api-sync-strategies' AND v2='read');
INSERT INTO casbin_rule (ptype, v0, v1, v2) SELECT 'p','operator','api-sync-strategies','write' WHERE NOT EXISTS (SELECT 1 FROM casbin_rule WHERE ptype='p' AND v0='operator' AND v1='api-sync-strategies' AND v2='write');
-- viewer: read
INSERT INTO casbin_rule (ptype, v0, v1, v2) SELECT 'p','viewer','api-sync-strategies','read' WHERE NOT EXISTS (SELECT 1 FROM casbin_rule WHERE ptype='p' AND v0='viewer' AND v1='api-sync-strategies' AND v2='read');
