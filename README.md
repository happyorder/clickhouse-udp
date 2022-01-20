# ClickHouse UDP ingestion

Written in go.

## Installation

1. Modify deploy.sh script with your ssh credentials.

2. Run deploy command:

```bash
./deploy.sh
```

3. Create systemd service for autostart (optional):

```bash
sudo nano /etc/systemd/system/clickhouse-udp.service
```

Paste contents of the file `clickhouse-udp.service` and replace user, group and executable path

```bash
sudo systemctl daemon-reload
sudo systemctl enable clickhouse-udp.service
sudo systemctl start clickhouse-udp.service
```

To check status

```bash
sudo systemctl status clickhouse-udp.service -l
```

To read logs

```bash
journalctl -u clickhouse-udp.service
```
