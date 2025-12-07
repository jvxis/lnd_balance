# Lightning Daily Fee Reporter

Python 3 script to connect to an LND node via gRPC, cache daily routing economics in SQLite, and report monthly profit:
- Forwarding fee income
- Rebalancing fee costs
- Net routing profit (forwards - rebalances)

## Requirements
- Python 3.9+
- LND gRPC enabled and reachable from this host
- TLS cert and macaroon for your node
- Generated LND stubs present in this repo: `lightning_pb2.py` and `lightning_pb2_grpc.py`
- Packages from `requirements.txt` (protobuf + grpcio)

## Install
Recommended: use a virtual environment.

```bash
python3 -m venv .venv
source .venv/bin/activate           # Windows PowerShell: .\.venv\Scripts\Activate.ps1
python3 -m pip install -r requirements.txt
```

## Required environment variables
Set these before running:
- `LND_GRPC_HOST` (e.g. `127.0.0.1` or `jvx-minipc01`)
- `LND_GRPC_PORT` (e.g. `10009`)
- `LND_TLS_CERT` (path to `tls.cert`)
- `LND_MACAROON` (path to macaroon, typically `.../data/chain/bitcoin/mainnet/admin.macaroon`)
- Optional: `TZ` (IANA timezone like `America/Sao_Paulo`; defaults to local time)

Example (Linux/macOS):
```bash
export LND_GRPC_HOST=jvx-minipc01
export LND_GRPC_PORT=10009
export LND_TLS_CERT="/home/admin/.lnd/tls.cert"
export LND_MACAROON="/home/admin/.lnd/data/chain/bitcoin/mainnet/admin.macaroon"
python3 lnd_daily_fees.py
```

PowerShell example:
```powershell
$env:LND_GRPC_HOST = "jvx-minipc01"
$env:LND_GRPC_PORT = "10009"
$env:LND_TLS_CERT = "C:\Users\admin\.lnd\tls.cert"
$env:LND_MACAROON = "C:\Users\admin\.lnd\data\chain\bitcoin\mainnet\admin.macaroon"
python3 lnd_daily_fees.py
```

## Usage
- Default: backfill from the first day of the month that is `--months` ago up to yesterday (local time). Only days missing in the SQLite DB are fetched and stored.
- Custom date range (inclusive, capped at yesterday):
  ```bash
  python3 lnd_daily_fees.py --from 2025-12-01 --to 2025-12-03
  ```
- Override timezone:
  ```bash
  python3 lnd_daily_fees.py --tz America/Sao_Paulo
  ```
- Choose database path (default: `lnd_fees.sqlite`):
  ```bash
  python3 lnd_daily_fees.py --db-path /path/to/fees.sqlite
  ```
- Monthly report depth: show current month plus N previous months (default 3):
  ```bash
  python3 lnd_daily_fees.py --months 6
  ```

## Output
The script prints:
- Which dates were inserted into the DB
- Node alias/pubkey (when connecting)
- Monthly revenue, rebalance costs, and profit for the current month plus `--months` previous months

## Troubleshooting
- `ModuleNotFoundError: google`: install protobuf via `pip install -r requirements.txt`.
- TLS/macaroon path errors: verify the file paths set in environment variables.
- Connection errors: confirm `LND_GRPC_HOST:PORT` is reachable and gRPC is enabled.
