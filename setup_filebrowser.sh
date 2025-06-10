#!/bin/bash
# Setup script for FileBrowser - A Windows Explorer-like web interface

echo "Setting up FileBrowser..."

# Create directories
mkdir -p /Stock_Project/data
mkdir -p /Stock_Project/filebrowser

# Download FileBrowser
cd /Stock_Project/filebrowser
curl -fsSL https://raw.githubusercontent.com/filebrowser/get/master/get.sh | bash

# Create FileBrowser configuration
cat > /Stock_Project/filebrowser/config.json << EOF
{
  "port": 8080,
  "baseURL": "",
  "address": "0.0.0.0",
  "log": "stdout",
  "database": "/Stock_Project/filebrowser/database.db",
  "root": "/Stock_Project/data",
  "username": "admin",
  "password": "",
  "locale": "en",
  "allow_commands": false,
  "allow_edit": true,
  "allow_new": true,
  "commands": []
}
EOF

# Create systemd service for auto-start
cat > /etc/systemd/system/filebrowser.service << EOF
[Unit]
Description=FileBrowser
After=network.target

[Service]
Type=simple
User=root
Group=root
ExecStart=/Stock_Project/filebrowser/filebrowser -c /Stock_Project/filebrowser/config.json
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

# Set executable permissions
chmod +x /Stock_Project/filebrowser/filebrowser
chmod +x /Stock_Project/setup_filebrowser.sh

echo "FileBrowser setup complete!"
echo ""
echo "To start FileBrowser:"
echo "  1. Run: systemctl daemon-reload"
echo "  2. Run: systemctl enable filebrowser"
echo "  3. Run: systemctl start filebrowser"
echo ""
echo "FileBrowser will be available at: http://YOUR_SERVER_IP:8080"
echo "Default login: admin/admin (change on first login!)"
echo ""
echo "To check status: systemctl status filebrowser"
echo "To view logs: journalctl -u filebrowser -f"
