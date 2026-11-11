#!/usr/bin/env bash
# Installs the ingestor as a systemd service on a Linux node (e.g. Raspberry Pi with Ubuntu Server 64-bit).
# Run from nodes/ingestor/:   sudo deploy/install.sh [--with-kafka]
# It changes the operating system (user, /opt, /etc, systemd): run it only with the maintainer's approval
# (AGENTS.md §4). It does not start the service; that is a separate, explicit step.
set -euo pipefail

PREFIX=/opt/mibel-ingestor
ETC=/etc/mibel-ingestor
SRC="$(cd "$(dirname "$0")/.." && pwd)"
WITH_KAFKA=0
[[ "${1:-}" == "--with-kafka" ]] && WITH_KAFKA=1

[[ $EUID -eq 0 ]] || { echo "run as root (sudo)"; exit 1; }
command -v python3 >/dev/null || { echo "python3 not found"; exit 1; }
python3 -c 'import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)' || { echo "Python >= 3.10 required"; exit 1; }

id mibel >/dev/null 2>&1 || useradd --system --home "$PREFIX" --shell /usr/sbin/nologin mibel
mkdir -p "$PREFIX" "$ETC"
rsync -a --delete --exclude '.state' --exclude 'benchmarks/results' --exclude '__pycache__' "$SRC/" "$PREFIX/"
python3 -m venv "$PREFIX/.venv"
"$PREFIX/.venv/bin/pip" install --quiet --upgrade pip
"$PREFIX/.venv/bin/pip" install --quiet -r "$PREFIX/requirements.lock.txt"
"$PREFIX/.venv/bin/pip" install --quiet --no-deps -e "$PREFIX"
[[ $WITH_KAFKA -eq 1 ]] && "$PREFIX/.venv/bin/pip" install --quiet "confluent-kafka>=2.3"
chown -R mibel:mibel "$PREFIX"

if [[ ! -f "$ETC/env" ]]; then
  install -m 0640 -o root -g mibel "$SRC/deploy/env.example" "$ETC/env"
  echo "edit $ETC/env with the real values before starting the service"
fi
install -m 0644 "$SRC/deploy/mibel-ingestor.service" /etc/systemd/system/mibel-ingestor.service
systemctl daemon-reload
echo "installed. Next: edit $ETC/env, then: systemctl enable --now mibel-ingestor"
