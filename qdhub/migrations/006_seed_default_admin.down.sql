-- Rollback: Remove default admin user
-- Version: 006

DELETE FROM user_roles WHERE user_id = '00000000-0000-0000-0000-000000000001';
DELETE FROM users WHERE username = 'admin';
