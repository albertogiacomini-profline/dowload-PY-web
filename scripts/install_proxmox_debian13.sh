#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/downloader-ui"
SERVICE_NAME="downloader-ui.service"

apt update
apt install -y --no-install-recommends \
  python3 \
  python3-venv \
  python3-pip \
  git

mkdir -p "${APP_DIR}"
cd "${APP_DIR}"

if [ ! -d ".git" ]; then
  git clone https://github.com/albertogiacomini-profline/dowload-PY-web .
fi

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

cat <<'EOF' >/etc/systemd/system/downloader-ui.service
[Unit]
Description=Downloader UI
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/downloader-ui
ExecStart=/opt/downloader-ui/.venv/bin/python /opt/downloader-ui/app.py
Restart=on-failure
Environment=DOWNLOADER_BASE_PATH=/mnt/anime
Environment=DOWNLOADER_DATA_DIR=/var/lib/downloader

[Install]
WantedBy=multi-user.target
EOF

mkdir -p /var/lib/downloader
systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

echo "App pronta su http://<IP_CONTAINER>:8080"
