#!/bin/bash

# scripts/install_snowsql.sh
# Installs SnowSQL and sets up configuration at $PROJECT_DIR/.snowsql/config
SCRIPT_VERSION="1.0.0"
# Enable strict mode for error handling
set -euo pipefail

# Constants
VERSION="1.4.4"
BOOTSTRAP_VERSION="1.4"
DOWNLOAD_URL="https://sfc-repo.snowflakecomputing.com/snowsql/bootstrap/${BOOTSTRAP_VERSION}/linux_x86_64/snowsql-${VERSION}-linux_x86_64.bash"
ALT_DOWNLOAD_URL="https://sfc-repo.azure.snowflakecomputing.com/snowsql/bootstrap/${BOOTSTRAP_VERSION}/linux_x86_64/snowsql-${VERSION}-linux_x86_64.bash"
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
CONFIG_DIR="$PROJECT_DIR/.snowsql"
CONFIG_FILE="$CONFIG_DIR/config"
SNOWSQL_BIN="$HOME/bin/snowsql"

# Function to print section headers
print_section() {
    echo ""
    echo "============================================================="
    echo "$1"
    echo "============================================================="
}

print_section "Starting SnowSQL installation"

# Install dependencies
if ! command -v curl >/dev/null || ! command -v gpg >/dev/null; then
    print_section "Installing dependencies (curl, gnupg)"
    apt-get update && apt-get install -y curl gnupg || {
        echo "ERROR: Failed to install dependencies"
        exit 1
    }
fi

# Download installer
cd /tmp || { echo "ERROR: Cannot access /tmp"; exit 1; }

print_section "Downloading SnowSQL installer"
if ! curl -f -O "$DOWNLOAD_URL"; then
    echo "Trying alternative Azure endpoint..."
    curl -f -O "$ALT_DOWNLOAD_URL" || {
        echo "ERROR: Failed to download SnowSQL installer"
        exit 1
    }
fi

print_section "Downloading signature file"
curl -f -O "${DOWNLOAD_URL}.sig" || curl -f -O "${ALT_DOWNLOAD_URL}.sig" || {
    echo "ERROR: Failed to download signature file"
    exit 1
}

print_section "Importing Snowflake GPG key"
gpg --keyserver hkp://keyserver.ubuntu.com --recv-keys 2A3149C82551A34A 2>/dev/null || {
    echo "Trying port 80 for GPG key..."
    gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 2A3149C82551A34A 2>/dev/null || {
        echo "ERROR: Failed to import GPG key"
        exit 1
    }
}

print_section "Verifying installer signature"
gpg --verify "snowsql-${VERSION}-linux_x86_64.bash.sig" "snowsql-${VERSION}-linux_x86_64.bash" 2>/dev/null || {
    echo "ERROR: Signature verification failed"
    rm -f "snowsql-${VERSION}-linux_x86_64.bash" "snowsql-${VERSION}-linux_x86_64.bash.sig"
    exit 1
}

print_section "Running SnowSQL installer"
echo "~/bin" | bash "snowsql-${VERSION}-linux_x86_64.bash" || {
    echo "ERROR: SnowSQL installation failed"
    rm -f "snowsql-${VERSION}-linux_x86_64.bash" "snowsql-${VERSION}-linux_x86_64.bash.sig"
    exit 1
}

rm -f "snowsql-${VERSION}-linux_x86_64.bash" "snowsql-${VERSION}-linux_x86_64.bash.sig"

print_section "Verifying SnowSQL version"
if [ ! -x "$SNOWSQL_BIN" ]; then
    echo "ERROR: SnowSQL not found in $SNOWSQL_BIN"
    exit 1
fi

SNOWSQL_VERSION=$("$SNOWSQL_BIN" --version 2>&1)
echo "Installed SnowSQL version: $SNOWSQL_VERSION"
if [[ "$SNOWSQL_VERSION" != *"Version: $VERSION"* ]]; then
    echo "ERROR: Expected SnowSQL version $VERSION, but got: $SNOWSQL_VERSION"
    exit 1
fi

echo "✅ SnowSQL installed successfully in $HOME/bin"

print_section "Setting up SnowSQL configuration in $CONFIG_DIR"
mkdir -p "$CONFIG_DIR" || {
    echo "ERROR: Failed to create config directory: $CONFIG_DIR"
    exit 1
}

if [ -f "$CONFIG_FILE" ]; then
    rm "$CONFIG_FILE"
    echo "Removed existing config file for clean setup"
fi

cat > "$CONFIG_FILE" <<EOF
[options]
log_level = DEBUG
log_file = $CONFIG_DIR/snowsql_rt.log
log_bootstrap_file = $CONFIG_DIR/snowsql_bootstrap.log

[connections.blockchair]
accountname = \${SNOWFLAKE_ACCOUNT}
username = \${SNOWFLAKE_USER}
password = \${SNOWFLAKE_PASSWORD}
rolename = \${SNOWFLAKE_ROLE}
warehousename = \${SNOWFLAKE_WAREHOUSE}
EOF

chmod 700 "$CONFIG_DIR"
chmod 600 "$CONFIG_FILE"

echo "✅ Config created at $CONFIG_FILE"
print_section "SnowSQL setup complete"


