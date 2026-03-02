#!/bin/bash
# QDHub Deployment Script
# Usage: ./scripts/deploy.sh [dev|prod]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default environment
ENV=${1:-dev}

# Configuration
APP_NAME="qdhub"
DEPLOY_DIR="/opt/${APP_NAME}"
SERVICE_NAME="${APP_NAME}"

# Validate environment
if [[ "$ENV" != "dev" && "$ENV" != "prod" ]]; then
    echo -e "${RED}Error: Invalid environment '$ENV'. Use 'dev' or 'prod'.${NC}"
    exit 1
fi

echo -e "${GREEN}Deploying QDHub to ${ENV} environment...${NC}"

# Detect OS and architecture
OS=$(uname -s | tr '[:upper:]' '[:lower:]')
ARCH=$(uname -m)

case "$ARCH" in
    x86_64) ARCH="amd64" ;;
    aarch64|arm64) ARCH="arm64" ;;
    *) echo -e "${RED}Unsupported architecture: $ARCH${NC}"; exit 1 ;;
esac

BINARY_NAME="${APP_NAME}-${OS}-${ARCH}"
if [[ "$OS" == "windows" ]]; then
    BINARY_NAME="${BINARY_NAME}.exe"
fi

echo -e "${YELLOW}Target: ${OS}/${ARCH}${NC}"

# Build the binary
echo -e "${YELLOW}Building ${BINARY_NAME}...${NC}"
make build-${OS} 2>/dev/null || make build

# Select config file
if [[ "$ENV" == "prod" ]]; then
    CONFIG_FILE="configs/config.prod.yaml"
else
    CONFIG_FILE="configs/config.yaml"
fi

# Check if config exists
if [[ ! -f "$CONFIG_FILE" ]]; then
    echo -e "${RED}Error: Config file not found: $CONFIG_FILE${NC}"
    exit 1
fi

echo -e "${YELLOW}Using config: $CONFIG_FILE${NC}"

# Run database migrations
run_migrations() {
    local config_file=$1
    
    if [[ ! -f "scripts/migrate.sh" ]]; then
        echo -e "${YELLOW}Warning: migrate.sh not found, skipping migrations.${NC}"
        return 0
    fi
    
    echo -e "${YELLOW}Running database migrations...${NC}"
    chmod +x scripts/migrate.sh
    
    if ./scripts/migrate.sh up "$config_file"; then
        echo -e "${GREEN}Migrations completed successfully.${NC}"
    else
        echo -e "${RED}Warning: Some migrations may have failed.${NC}"
        return 1
    fi
}

# Run migrations with the selected config file
run_migrations "$CONFIG_FILE" || true

# For local development, just run the binary
if [[ "$ENV" == "dev" ]]; then
    echo -e "${GREEN}Starting ${APP_NAME} in development mode...${NC}"
    ./bin/${APP_NAME} server --config "$CONFIG_FILE" --mode debug
    exit 0
fi

# Production deployment (requires proper permissions)
echo -e "${YELLOW}Production deployment...${NC}"

# Create deployment directory
sudo mkdir -p "$DEPLOY_DIR"
sudo mkdir -p "$DEPLOY_DIR/data"
sudo mkdir -p "$DEPLOY_DIR/configs"
sudo mkdir -p "$DEPLOY_DIR/logs"

# Copy binary and config
sudo cp "bin/${BINARY_NAME}" "$DEPLOY_DIR/${APP_NAME}"
sudo cp "$CONFIG_FILE" "$DEPLOY_DIR/configs/config.yaml"

# Set permissions
sudo chmod +x "$DEPLOY_DIR/${APP_NAME}"

# Create systemd service (Linux only)
if [[ "$OS" == "linux" ]]; then
    echo -e "${YELLOW}Creating systemd service...${NC}"
    
    sudo tee /etc/systemd/system/${SERVICE_NAME}.service > /dev/null <<EOF
[Unit]
Description=QDHub Quantitative Data Hub
After=network.target

[Service]
Type=simple
User=qdhub
Group=qdhub
WorkingDirectory=${DEPLOY_DIR}
ExecStart=${DEPLOY_DIR}/${APP_NAME} server --config ${DEPLOY_DIR}/configs/config.yaml
Restart=on-failure
RestartSec=5
StandardOutput=append:${DEPLOY_DIR}/logs/stdout.log
StandardError=append:${DEPLOY_DIR}/logs/stderr.log

[Install]
WantedBy=multi-user.target
EOF

    # Reload systemd and restart service
    sudo systemctl daemon-reload
    sudo systemctl enable ${SERVICE_NAME}
    sudo systemctl restart ${SERVICE_NAME}
    
    echo -e "${GREEN}Service ${SERVICE_NAME} started.${NC}"
    sudo systemctl status ${SERVICE_NAME} --no-pager
fi

echo -e "${GREEN}Deployment completed successfully!${NC}"
