#!/bin/bash

echo "🚀 Full Auto Setup Start..."

apt update -y
apt install -y python3 python3-pip wget

pip3 install --upgrade pip
pip3 install requests aiohttp

wget -O /root/worker.py https://raw.githubusercontent.com/amritmandal0187-sys/Ubuntu/main/worker_linux.py
chmod +x /root/worker.py

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

systemctl daemon-reexec
systemctl daemon-reload
systemctl enable worker
systemctl start worker

echo "✅ DONE: Auto start enabled"
