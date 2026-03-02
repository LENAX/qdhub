-- Seed default admin user (SQLite)
-- Version: 006
-- Description: Insert default admin account. Username=admin, password=admin123 (e2e 见 server_e2e_test.go e2eAdminUsername/e2eAdminPassword). 生产环境请首次登录后修改密码。

INSERT OR IGNORE INTO users (id, username, email, password_hash, status, created_at, updated_at)
VALUES (
    '00000000-0000-0000-0000-000000000001',
    'admin',
    'admin@localhost',
    '$2a$10$6dYpz/fbv/j2sGdRZCEHZO.IU.16UjeBsUMPEqN59gOuT3Vh2mYvG',
    'active',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO user_roles (user_id, role)
VALUES ('00000000-0000-0000-0000-000000000001', 'admin');
