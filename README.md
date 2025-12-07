# Lightning Daily Fee Reporter

Python 3 script to connect to an LND node via gRPC and report daily routing economics:
- Forwarding fee income
- Rebalancing fee costs
- Net routing profit

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
- `LND_GRPC_HOST` (e.g. `127.0.0.1` or `localhost`)
- `LND_GRPC_PORT` (e.g. `10009`)
- `LND_TLS_CERT` (path to `tls.cert`)
- `LND_MACAROON` (path to macaroon, typically `.../data/chain/bitcoin/mainnet/admin.macaroon`)
- Optional: `TZ` (IANA timezone like `America/Sao_Paulo`; defaults to local time)

Example (Linux/macOS):
```bash
export LND_GRPC_HOST=localhost
export LND_GRPC_PORT=10009
export LND_TLS_CERT="/home/admin/.lnd/tls.cert"
export LND_MACAROON="/home/admin/.lnd/data/chain/bitcoin/mainnet/admin.macaroon"
python3 lnd_daily_fees.py
```

PowerShell example:
```powershell
$env:LND_GRPC_HOST = "name_machine"
$env:LND_GRPC_PORT = "10009"
$env:LND_TLS_CERT = "C:\Users\admin\.lnd\tls.cert"
$env:LND_MACAROON = "C:\Users\admin\.lnd\data\chain\bitcoin\mainnet\admin.macaroon"
python3 lnd_daily_fees.py
```

## Usage
Default (previous calendar day in local time):
```bash
python3 lnd_daily_fees.py
```

Custom date range (inclusive, local calendar days):
```bash
python3 lnd_daily_fees.py --from 2025-12-01 --to 2025-12-03
```

Override timezone:
```bash
python3 lnd_daily_fees.py --tz America/Sao_Paulo
```

## Output
The script prints:
- Node alias/pubkey
- Local and UTC date range
- Forwarding fee income (total and per outbound channel)
- Rebalance fee costs (total)
- Net routing profit (forwards − rebalances)

## Troubleshooting
- `ModuleNotFoundError: google`: install protobuf via `pip install -r requirements.txt`.
- TLS/macaroon path errors: verify the file paths set in environment variables.
- Connection errors: confirm `LND_GRPC_HOST:PORT` is reachable and gRPC is enabled.***
