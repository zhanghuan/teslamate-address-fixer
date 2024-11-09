# Notice
TeslaMate-addr-fixer is a personal tool to fix broken addresses issue in China.

# Usage

## Prerequisite

### 1. Backup database
**Must backup the database before running the script.**

```bash
docker compose -f /path/to/docker-compose.yml exec -T database pg_dump -U teslamate teslamate > /path/to/backup.sql
```

### 2. Install required modules

```bash
pip3 install -r requirements.txt
```
## Run the script

Run script in teslamate host.

```bash
python3 main.py fix -w <db-password> -x "<https_proxy_ip>:<port>"
```

Run with '--help' for more info.

# Design

* Related tables need to query for missing addresses:
| Table | Description |
| --- | --- |
| drives | contains starting and ending addresses per drive |
| charging_processes | contains addresses per charge |
| positions | contains latitude/longitude per position |
| addresses | contains address info for starting/ending address per drive and address per charge. address is uniqued by osm_id and osm_type. |


# Credits

This project is inspired by [teslamate-address-fixer](https://github.com/WayneJz/teslamate-addr-fix).

# Disclaimer

This project is for personal use only. Use at your own risk.
