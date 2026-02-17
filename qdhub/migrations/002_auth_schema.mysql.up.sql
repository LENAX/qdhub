-- Authentication and RBAC schema for MySQL

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id VARCHAR(36) PRIMARY KEY,
    username VARCHAR(64) UNIQUE NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    status VARCHAR(16) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- User roles table
CREATE TABLE IF NOT EXISTS user_roles (
    user_id VARCHAR(36) NOT NULL,
    role VARCHAR(32) NOT NULL,
    PRIMARY KEY (user_id, role),
    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Casbin rules table (for RBAC policies)
CREATE TABLE IF NOT EXISTS casbin_rule (
    id INT AUTO_INCREMENT PRIMARY KEY,
    ptype VARCHAR(16),
    v0 VARCHAR(256),
    v1 VARCHAR(256),
    v2 VARCHAR(256),
    v3 VARCHAR(256),
    v4 VARCHAR(256),
    v5 VARCHAR(256),
    INDEX idx_casbin_rule (ptype, v0, v1)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
