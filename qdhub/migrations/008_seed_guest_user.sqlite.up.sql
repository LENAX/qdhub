-- Seed guest user (SQLite)
-- Version: 008
-- Description: Insert guest account. Username=guest, password=guest123, role=viewer（仅能查看数据）. 生产环境可禁用或修改密码。

INSERT OR IGNORE INTO users (id, username, email, password_hash, status, created_at, updated_at)
VALUES (
    '00000000-0000-0000-0000-000000000002',
    'guest',
    'guest@localhost',
    '$2a$10$lh/7GN7/gkyW0p2PG42Cke5kR2fX/VDd.W5AaMhPMPzXZ/4Au4AVK',
    'active',
    CURRENT_TIMESTAMP,
    CURRENT_TIMESTAMP
);

INSERT OR IGNORE INTO user_roles (user_id, role)
VALUES ('00000000-0000-0000-0000-000000000002', 'viewer');
