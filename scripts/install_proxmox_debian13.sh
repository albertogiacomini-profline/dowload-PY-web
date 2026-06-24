#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/opt/downloader-ui"
DATA_DIR="/var/lib/downloader"
APP_USER="downloader"
SERVICE_NAME="downloader-ui.service"

apt update
apt install -y --no-install-recommends \
  python3 \
  python3-venv \
  python3-pip \
  git \
  openssl

mkdir -p "${APP_DIR}"
cd "${APP_DIR}"

if [ ! -d ".git" ]; then
  git clone https://github.com/albertogiacomini-profline/dowload-PY-web .
fi

python3 -m venv .venv
source .venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt

id -u "${APP_USER}" >/dev/null 2>&1 || useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${APP_USER}"
mkdir -p "${DATA_DIR}"
if [ ! -f "${DATA_DIR}/secret_key" ]; then
  openssl rand -base64 48 > "${DATA_DIR}/secret_key"
fi
chown -R "${APP_USER}:${APP_USER}" "${DATA_DIR}"
chmod 700 "${DATA_DIR}"
chmod 600 "${DATA_DIR}/secret_key"

cat <<'EOF' >/etc/systemd/system/downloader-ui.service
[Unit]
Description=Downloader UI
After=network.target

[Service]
Type=simple
WorkingDirectory=/opt/downloader-ui
User=downloader
Group=downloader
ExecStart=/opt/downloader-ui/.venv/bin/gunicorn --workers 1 --threads 4 --bind 0.0.0.0:8080 app:app
Restart=on-failure
Environment=DOWNLOADER_BASE_PATH=/mnt/anime
Environment=DOWNLOADER_DATA_DIR=/var/lib/downloader
Environment=DOWNLOADER_SECRET_KEY_FILE=/var/lib/downloader/secret_key

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now "${SERVICE_NAME}"

echo "App pronta su http://<IP_CONTAINER>:8080"
