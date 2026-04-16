#!/bin/bash

echo "🚀 Starting full auto setup..."

# Update system
apt update -y

# Install basics
apt install -y python3 python3-pip wget

# Install Python modules
pip3 install --upgrade pip
pip3 install requests aiohttp

# Download worker script
wget -O /root/worker.py https://raw.githubusercontent.com/amritmandal0187-sys/Ubuntu/main/worker_linux.py

# Give permission
chmod +x /root/worker.py

# Create systemd service
cat <<EOF > /etc/systemd/system/worker.service
[Unit]
Description=Worker Script
After=network.target

[Service]
ExecStart=/usr/bin/python3 /root/worker.py
Restart=always
User=root

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd
systemctl daemon-reexec
systemctl daemon-reload

# Enable service
systemctl enable worker

# Start service
systemctl start worker

echo "✅ Setup completed!"
echo "🔁 Worker is now running & will auto start on reboot."
