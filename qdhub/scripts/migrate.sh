#!/bin/bash
# QDHub Database Migration Script
# Usage: ./scripts/migrate.sh [up|down|status] [config_file]
#
# This script manages database migrations for multiple database types.
# It reads database configuration from the config file and tracks applied
# migrations in a schema_migrations table.
#
# Supported databases: sqlite, postgres, mysql

set -e

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

ACTION=${1:-status}
CONFIG_FILE=${2:-"configs/config.yaml"}
MIGRATIONS_DIR="migrations"

# Ensure we're in the project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR/.."

echo -e "${YELLOW}Database Migration: ${ACTION}${NC}"

# Parse YAML config to extract database settings
# Uses simple grep/sed for portability (no external YAML parser needed)
parse_config() {
    if [[ ! -f "$CONFIG_FILE" ]]; then
        echo -e "${RED}Error: Config file not found: $CONFIG_FILE${NC}"
        exit 1
    fi
    
    # Extract database driver and dsn from config
    DB_DRIVER=$(grep -A5 "^database:" "$CONFIG_FILE" | grep "driver:" | sed 's/.*driver:[[:space:]]*"\?\([^"]*\)"\?.*/\1/' | tr -d ' ')
    DB_DSN=$(grep -A5 "^database:" "$CONFIG_FILE" | grep "dsn:" | sed 's/.*dsn:[[:space:]]*"\?\([^"]*\)"\?.*/\1/' | tr -d ' ')
    
    # Expand environment variables in DSN
    DB_DSN=$(eval echo "$DB_DSN")
    
    if [[ -z "$DB_DRIVER" ]]; then
        echo -e "${RED}Error: Database driver not found in config${NC}"
        exit 1
    fi
    
    echo -e "${YELLOW}Database driver: ${DB_DRIVER}${NC}"
}

# Execute SQL based on database type
exec_sql() {
    local sql=$1
    
    case "$DB_DRIVER" in
        sqlite|sqlite3)
            sqlite3 "$DB_DSN" "$sql"
            ;;
        postgres|postgresql)
            PGPASSWORD="${PGPASSWORD:-}" psql "$DB_DSN" -t -c "$sql" 2>/dev/null
            ;;
        mysql)
            mysql "$DB_DSN" -N -e "$sql" 2>/dev/null
            ;;
        *)
            echo -e "${RED}Error: Unsupported database driver: $DB_DRIVER${NC}"
            exit 1
            ;;
    esac
}

# Preprocess SQL for database compatibility
# Converts "INSERT OR IGNORE" syntax to database-specific syntax
preprocess_sql() {
    local file=$1
    local temp_file=$(mktemp)
    
    case "$DB_DRIVER" in
        sqlite|sqlite3)
            # SQLite: INSERT OR IGNORE is native syntax
            cat "$file" > "$temp_file"
            ;;
        postgres|postgresql)
            # PostgreSQL: Convert "INSERT OR IGNORE INTO table" to "INSERT INTO table ... ON CONFLICT DO NOTHING"
            # This is a simplified conversion that works for our seed data pattern
            sed 's/INSERT OR IGNORE INTO/INSERT INTO/g' "$file" | \
            sed 's/);$/) ON CONFLICT DO NOTHING;/g' > "$temp_file"
            ;;
        mysql)
            # MySQL: Convert "INSERT OR IGNORE" to "INSERT IGNORE"
            sed 's/INSERT OR IGNORE/INSERT IGNORE/g' "$file" > "$temp_file"
            ;;
        *)
            cat "$file" > "$temp_file"
            ;;
    esac
    
    echo "$temp_file"
}

# Execute SQL file based on database type
exec_sql_file() {
    local file=$1
    local processed_file=$(preprocess_sql "$file")
    local result=0
    
    case "$DB_DRIVER" in
        sqlite|sqlite3)
            sqlite3 "$DB_DSN" < "$processed_file" || result=$?
            ;;
        postgres|postgresql)
            PGPASSWORD="${PGPASSWORD:-}" psql "$DB_DSN" -f "$processed_file" || result=$?
            ;;
        mysql)
            mysql "$DB_DSN" < "$processed_file" || result=$?
            ;;
        *)
            echo -e "${RED}Error: Unsupported database driver: $DB_DRIVER${NC}"
            rm -f "$processed_file"
            exit 1
            ;;
    esac
    
    rm -f "$processed_file"
    return $result
}

# Check if database client is available
check_db_client() {
    case "$DB_DRIVER" in
        sqlite|sqlite3)
            if ! command -v sqlite3 &> /dev/null; then
                echo -e "${RED}Error: sqlite3 is not installed.${NC}"
                exit 1
            fi
            # Create directory for SQLite if needed
            local db_dir=$(dirname "$DB_DSN")
            if [[ ! -d "$db_dir" && "$db_dir" != "." ]]; then
                echo -e "${YELLOW}Creating database directory: ${db_dir}${NC}"
                mkdir -p "$db_dir"
            fi
            ;;
        postgres|postgresql)
            if ! command -v psql &> /dev/null; then
                echo -e "${RED}Error: psql (PostgreSQL client) is not installed.${NC}"
                exit 1
            fi
            ;;
        mysql)
            if ! command -v mysql &> /dev/null; then
                echo -e "${RED}Error: mysql client is not installed.${NC}"
                exit 1
            fi
            ;;
    esac
}

# Initialize schema_migrations table if not exists
init_migrations_table() {
    case "$DB_DRIVER" in
        sqlite|sqlite3)
            exec_sql "CREATE TABLE IF NOT EXISTS schema_migrations (version TEXT PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            ;;
        postgres|postgresql)
            exec_sql "CREATE TABLE IF NOT EXISTS schema_migrations (version VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            ;;
        mysql)
            exec_sql "CREATE TABLE IF NOT EXISTS schema_migrations (version VARCHAR(255) PRIMARY KEY, applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP);"
            ;;
    esac
}

# Check if a migration has been applied
is_migration_applied() {
    local version=$1
    local count=$(exec_sql "SELECT COUNT(*) FROM schema_migrations WHERE version = '$version';")
    count=$(echo "$count" | tr -d ' \n\r')
    [[ "$count" -gt 0 ]]
}

# Record migration as applied
record_migration() {
    local version=$1
    exec_sql "INSERT INTO schema_migrations (version) VALUES ('$version');"
}

# Remove migration record
remove_migration_record() {
    local version=$1
    exec_sql "DELETE FROM schema_migrations WHERE version = '$version';"
}

# Get version from filename (e.g., 001_init_schema.up.sql -> 001_init_schema)
get_version() {
    local filename=$1
    basename "$filename" | sed 's/\.\(up\|down\)\.sql$//'
}

# Get last applied migration
get_last_migration() {
    exec_sql "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1;" | tr -d ' \n\r'
}

# Parse config
parse_config
check_db_client

case "$ACTION" in
    up)
        echo -e "${GREEN}Applying all pending migrations...${NC}"
        init_migrations_table
        
        applied_count=0
        for file in $(ls -1 ${MIGRATIONS_DIR}/*.up.sql 2>/dev/null | sort); do
            version=$(get_version "$file")
            
            if is_migration_applied "$version"; then
                echo -e "  ${YELLOW}Skip (already applied):${NC} $version"
                continue
            fi
            
            echo -e "  ${GREEN}Applying:${NC} $file"
            if exec_sql_file "$file" 2>&1; then
                record_migration "$version"
                echo -e "  ${GREEN}✓ Applied:${NC} $version"
                ((applied_count++)) || true
            else
                echo -e "  ${RED}✗ Failed:${NC} $version"
                exit 1
            fi
        done
        
        if [[ $applied_count -eq 0 ]]; then
            echo -e "${GREEN}No pending migrations.${NC}"
        else
            echo -e "${GREEN}Applied ${applied_count} migration(s).${NC}"
        fi
        ;;
        
    down)
        echo -e "${YELLOW}Rolling back last migration...${NC}"
        init_migrations_table
        
        last_version=$(get_last_migration)
        
        if [[ -z "$last_version" ]]; then
            echo -e "${YELLOW}No migrations to rollback.${NC}"
            exit 0
        fi
        
        down_file="${MIGRATIONS_DIR}/${last_version}.down.sql"
        
        if [[ ! -f "$down_file" ]]; then
            echo -e "${RED}Error: Down migration not found: $down_file${NC}"
            exit 1
        fi
        
        echo -e "  ${YELLOW}Rolling back:${NC} $last_version"
        if exec_sql_file "$down_file" 2>&1; then
            remove_migration_record "$last_version"
            echo -e "  ${GREEN}✓ Rolled back:${NC} $last_version"
        else
            echo -e "  ${RED}✗ Rollback failed:${NC} $last_version"
            exit 1
        fi
        ;;
        
    status)
        echo -e "${YELLOW}Migration status:${NC}"
        init_migrations_table
        
        echo ""
        echo "Applied migrations:"
        case "$DB_DRIVER" in
            sqlite|sqlite3)
                sqlite3 "$DB_DSN" "SELECT version, applied_at FROM schema_migrations ORDER BY version;" 2>/dev/null | while read -r line; do
                    echo "  ✓ $line"
                done
                ;;
            postgres|postgresql)
                PGPASSWORD="${PGPASSWORD:-}" psql "$DB_DSN" -t -c "SELECT version || ' | ' || applied_at FROM schema_migrations ORDER BY version;" 2>/dev/null | while read -r line; do
                    [[ -n "$line" ]] && echo "  ✓ $line"
                done
                ;;
            mysql)
                mysql "$DB_DSN" -N -e "SELECT CONCAT(version, ' | ', applied_at) FROM schema_migrations ORDER BY version;" 2>/dev/null | while read -r line; do
                    echo "  ✓ $line"
                done
                ;;
        esac
        
        echo ""
        echo "Available migrations:"
        for file in $(ls -1 ${MIGRATIONS_DIR}/*.up.sql 2>/dev/null | sort); do
            version=$(get_version "$file")
            if is_migration_applied "$version"; then
                echo -e "  ${GREEN}✓${NC} $version (applied)"
            else
                echo -e "  ${YELLOW}○${NC} $version (pending)"
            fi
        done
        ;;
        
    *)
        echo "Usage: $0 [up|down|status] [config_file]"
        echo ""
        echo "Commands:"
        echo "  up      Apply all pending migrations"
        echo "  down    Rollback the last applied migration"
        echo "  status  Show migration status"
        echo ""
        echo "Options:"
        echo "  config_file  Path to config file (default: configs/config.yaml)"
        echo ""
        echo "Supported databases:"
        echo "  - sqlite (sqlite3)"
        echo "  - postgres (postgresql)"
        echo "  - mysql"
        echo ""
        echo "Examples:"
        echo "  $0 up                              # Use default config"
        echo "  $0 up configs/config.prod.yaml     # Use production config"
        echo "  $0 status                          # Show migration status"
        exit 1
        ;;
esac
