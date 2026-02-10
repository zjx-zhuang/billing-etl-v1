# ClickHouse Client Tool

A Python-based CLI tool to interact with the billing ClickHouse database.

## Prerequisites

- Python 3.8+
- `clickhouse-driver` (installed via requirements.txt)

## Installation

```bash
pip install -r requirements.txt
```

## Usage

The tool loads configuration from `config.yaml` by default.

### Configuration

You can modify the `config.yaml` file to set your default connection details:

```yaml
clickhouse:
  host: "34.21.0.33"
  port: 9000
  user: "billing"
  password: "..."
  database: "billing"
  secure: true
  verify: false
```

### List Tables

```bash
python main.py --list-tables
```

### Execute Query

```bash
python main.py --query "SELECT * FROM ods_billing LIMIT 5"
```

### Custom Connection

You can override any configuration parameter:

```bash
python main.py --host <HOST> --port <PORT> --user <USER> --password <PASS> --database <DB>
```

## Connection Details

- **Host**: 34.21.0.33
- **Port**: 9000 (Secure/SSL enabled, verification disabled by default)
- **User**: billing
- **Database**: billing
