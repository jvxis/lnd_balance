"""
Lightning Network daily fee report helper.

This script connects to an LND node via gRPC, fetches forwarding events and
outgoing payments, computes routing income (forwards) versus costs
(rebalancing), and caches daily results in a local SQLite database. By default
it processes the previous local calendar day ("D-1"), backfills missing days
within a requested range up to yesterday, and produces a monthly profit
summary (current month + previous months via --months).

Requirements:
- Python 3.9+
- grpcio
- LND-generated stubs: lightning_pb2.py, lightning_pb2_grpc.py (ForwardingHistory
  is available on the Lightning service, so router stubs are not required here).

Environment variables (all required):
- LND_GRPC_HOST (e.g. "127.0.0.1")
- LND_GRPC_PORT (e.g. "10009")
- LND_TLS_CERT (path to tls.cert)
- LND_MACAROON (path to admin macaroon)
- Optional: TZ (IANA timezone name, e.g. "America/Sao_Paulo"); if omitted,
  the local timezone is auto-detected.
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Dict, List, Optional, Tuple

import grpc
from zoneinfo import ZoneInfo

import lightning_pb2  # type: ignore
import lightning_pb2_grpc  # type: ignore
# ------------------------------------------------------------------------------
# Data classes
# ------------------------------------------------------------------------------

@dataclass
class EnvConfig:
    host: str
    port: str
    tls_cert_path: str
    macaroon_path: str
    tz_name: Optional[str]


@dataclass
class TimeRange:
    start_ts_utc: int
    end_ts_utc: int
    start_local: datetime
    end_local: datetime


@dataclass
class RebalanceDetail:
    payment_hash: str
    amount_sat: int
    fee_sat: int
    hop_count: int


@dataclass
class DailyFeeRecord:
    date_str: str
    forward_fees_sat: int
    rebalance_fees_sat: int
    net_profit_sat: int
    start_ts_utc: int
    end_ts_utc: int


# ------------------------------------------------------------------------------
# Utility helpers
# ------------------------------------------------------------------------------

def print_error(message: str) -> None:
    """Prints an error message to stderr."""
    sys.stderr.write(f"Error: {message}\n")


def load_env_config() -> Optional[EnvConfig]:
    """
    Load required environment variables. Returns None if any are missing.
    """
    required_vars = ["LND_GRPC_HOST", "LND_GRPC_PORT", "LND_TLS_CERT", "LND_MACAROON"]
    missing = [name for name in required_vars if not os.environ.get(name)]
    if missing:
        print_error(f"Missing required environment variables: {', '.join(missing)}")
        return None

    return EnvConfig(
        host=os.environ["LND_GRPC_HOST"],
        port=os.environ["LND_GRPC_PORT"],
        tls_cert_path=os.environ["LND_TLS_CERT"],
        macaroon_path=os.environ["LND_MACAROON"],
        tz_name=os.environ.get("TZ"),
    )


def resolve_timezone(tz_name: Optional[str]) -> timezone:
    """
    Resolve a timezone either from the provided name or the local system.
    Falls back to UTC if resolution fails.
    """
    if tz_name:
        try:
            return ZoneInfo(tz_name)
        except Exception:
            print_error(f"Could not resolve timezone '{tz_name}', falling back to local.")

    local_tz = datetime.now().astimezone().tzinfo
    if local_tz is None:
        print_error("Could not detect local timezone, falling back to UTC.")
        return timezone.utc
    return local_tz


def get_time_range_dminus1(tz_name: Optional[str]) -> TimeRange:
    """
    Compute the previous local calendar day and convert to UTC timestamps.
    Returns start/end timestamps (UTC seconds) and the local datetime bounds.
    """
    tzinfo = resolve_timezone(tz_name)
    now_local = datetime.now(tzinfo)
    target_date = (now_local - timedelta(days=1)).date()

    start_local = datetime.combine(target_date, time(0, 0, 0), tzinfo)
    end_local = datetime.combine(target_date, time(23, 59, 59), tzinfo)

    start_ts_utc = int(start_local.astimezone(timezone.utc).timestamp())
    end_ts_utc = int(end_local.astimezone(timezone.utc).timestamp())

    return TimeRange(
        start_ts_utc=start_ts_utc,
        end_ts_utc=end_ts_utc,
        start_local=start_local,
        end_local=end_local,
    )


def get_time_range_for_date(target_date: date, tz_name: Optional[str]) -> TimeRange:
    """
    Compute local start/end for a specific date and convert to UTC timestamps.
    """
    tzinfo = resolve_timezone(tz_name)
    start_local = datetime.combine(target_date, time(0, 0, 0), tzinfo)
    end_local = datetime.combine(target_date, time(23, 59, 59), tzinfo)
    start_ts_utc = int(start_local.astimezone(timezone.utc).timestamp())
    end_ts_utc = int(end_local.astimezone(timezone.utc).timestamp())

    return TimeRange(
        start_ts_utc=start_ts_utc,
        end_ts_utc=end_ts_utc,
        start_local=start_local,
        end_local=end_local,
    )


def get_time_range_from_cli(
    start_str: str, end_str: str, tz_name: Optional[str]
) -> Optional[TimeRange]:
    """
    Build a time range from CLI-provided date strings (YYYY-MM-DD).
    Returns None on validation error.
    """
    tzinfo = resolve_timezone(tz_name)
    try:
        start_date = datetime.strptime(start_str, "%Y-%m-%d").date()
        end_date = datetime.strptime(end_str, "%Y-%m-%d").date()
    except ValueError:
        print_error("Dates must be in YYYY-MM-DD format.")
        return None

    if end_date < start_date:
        print_error("The --to date must be on or after the --from date.")
        return None

    start_local = datetime.combine(start_date, time(0, 0, 0), tzinfo)
    end_local = datetime.combine(end_date, time(23, 59, 59), tzinfo)

    start_ts_utc = int(start_local.astimezone(timezone.utc).timestamp())
    end_ts_utc = int(end_local.astimezone(timezone.utc).timestamp())

    return TimeRange(
        start_ts_utc=start_ts_utc,
        end_ts_utc=end_ts_utc,
        start_local=start_local,
        end_local=end_local,
    )


# ------------------------------------------------------------------------------
# SQLite helpers
# ------------------------------------------------------------------------------


def init_db(db_path: str) -> sqlite3.Connection:
    """Create/open the SQLite database and ensure schema exists."""
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS daily_fees (
            date TEXT PRIMARY KEY,
            forward_fees_sat INTEGER NOT NULL,
            rebalance_fees_sat INTEGER NOT NULL,
            net_profit_sat INTEGER NOT NULL,
            start_ts_utc INTEGER NOT NULL,
            end_ts_utc INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    return conn


def day_exists(conn: sqlite3.Connection, date_str: str) -> bool:
    """Return True if a row already exists for the given YYYY-MM-DD date."""
    cur = conn.execute("SELECT 1 FROM daily_fees WHERE date = ? LIMIT 1", (date_str,))
    return cur.fetchone() is not None


def insert_daily_fee(conn: sqlite3.Connection, record: DailyFeeRecord) -> None:
    """Insert a single day's fee summary."""
    conn.execute(
        """
        INSERT OR REPLACE INTO daily_fees
        (date, forward_fees_sat, rebalance_fees_sat, net_profit_sat, start_ts_utc, end_ts_utc)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            record.date_str,
            record.forward_fees_sat,
            record.rebalance_fees_sat,
            record.net_profit_sat,
            record.start_ts_utc,
            record.end_ts_utc,
        ),
    )


def iter_dates(start_date: date, end_date: date) -> List[date]:
    """Inclusive list of dates from start_date to end_date."""
    days = []
    cur = start_date
    while cur <= end_date:
        days.append(cur)
        cur += timedelta(days=1)
    return days


def fetch_daily_row(conn: sqlite3.Connection, date_str: str) -> Optional[DailyFeeRecord]:
    """Fetch a stored daily record."""
    cur = conn.execute(
        """
        SELECT date, forward_fees_sat, rebalance_fees_sat, net_profit_sat, start_ts_utc, end_ts_utc
        FROM daily_fees
        WHERE date = ?
        """,
        (date_str,),
    )
    row = cur.fetchone()
    if not row:
        return None
    return DailyFeeRecord(
        date_str=row[0],
        forward_fees_sat=int(row[1]),
        rebalance_fees_sat=int(row[2]),
        net_profit_sat=int(row[3]),
        start_ts_utc=int(row[4]),
        end_ts_utc=int(row[5]),
    )


def month_keys(current_date: date, months_total: int) -> List[str]:
    """
    Return a list of YYYY-MM keys for current month plus prior months, length = months_total.
    months_total=1 yields only the current month.
    """
    months_total = max(months_total, 1)
    keys: List[str] = []
    year = current_date.year
    month = current_date.month
    for _ in range(months_total):
        keys.append(f"{year:04d}-{month:02d}")
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return keys


def aggregate_monthly(
    conn: sqlite3.Connection, tzinfo: timezone, months_total: int
) -> List[Tuple[str, int, int, int]]:
    """
    Aggregate totals by month for current month and previous months.
    Returns a list of (month_key, revenue, rebalance_cost, profit) sorted newest first,
    with length == months_total.
    """
    if months_total < 1:
        months_total = 1
    today_local = datetime.now(tzinfo).date()
    keys = month_keys(today_local, months_total)

    cur = conn.execute(
        """
        SELECT substr(date, 1, 7) as ym,
               SUM(forward_fees_sat) as revenue,
               SUM(rebalance_fees_sat) as cost,
               SUM(net_profit_sat) as profit
        FROM daily_fees
        GROUP BY ym
        """
    )
    rows = cur.fetchall()
    totals = {row[0]: (int(row[1] or 0), int(row[2] or 0), int(row[3] or 0)) for row in rows}

    result: List[Tuple[str, int, int, int]] = []
    for key in keys:
        revenue, cost, profit = totals.get(key, (0, 0, 0))
        result.append((key, revenue, cost, profit))
    return result


def first_day_for_months_back(today_local: date, months_back: int) -> date:
    """Return the first day of the month that is `months_back` before the current month."""
    months_back = max(months_back, 0)
    year = today_local.year
    month = today_local.month
    for _ in range(months_back):
        month -= 1
        if month == 0:
            month = 12
            year -= 1
    return date(year, month, 1)


def resolve_date_bounds(
    args: argparse.Namespace, tz_name: Optional[str]
) -> Optional[Tuple[date, date, timezone]]:
    """
    Determine the date range to process, capped at yesterday (local).
    If --from/--to not provided, backfills from the first day of the
    month that is (months_total-1) months ago, through yesterday.
    Returns (start_date, end_date, tzinfo).
    """
    tzinfo = resolve_timezone(tz_name)
    today_local = datetime.now(tzinfo).date()
    yesterday = today_local - timedelta(days=1)
    months_total = max(getattr(args, "months", 1) or 1, 1)
    months_back = months_total - 1

    if args.from_date and args.to_date:
        try:
            start_date = datetime.strptime(args.from_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(args.to_date, "%Y-%m-%d").date()
        except ValueError:
            print_error("Dates must be in YYYY-MM-DD format.")
            return None
    elif args.from_date or args.to_date:
        print_error("Both --from and --to must be provided together.")
        return None
    else:
        start_date = first_day_for_months_back(today_local, months_back)
        end_date = yesterday

    if end_date > yesterday:
        end_date = yesterday

    if start_date > end_date:
        print_error("Date range is empty (start is after end).")
        return None

    return start_date, end_date, tzinfo


def create_grpc_channel(config: EnvConfig) -> Optional[grpc.Channel]:
    """
    Build a secure gRPC channel with TLS + macaroon auth.
    """
    try:
        with open(config.tls_cert_path, "rb") as f:
            tls_cert = f.read()
    except OSError as exc:
        print_error(f"Unable to read TLS certificate: {exc}")
        return None

    try:
        with open(config.macaroon_path, "rb") as f:
            macaroon_bytes = f.read()
    except OSError as exc:
        print_error(f"Unable to read macaroon: {exc}")
        return None

    macaroon_hex = macaroon_bytes.hex()

    def metadata_callback(context, callback):
        # Adds macaroon to every request.
        callback((("macaroon", macaroon_hex),), None)

    ssl_creds = grpc.ssl_channel_credentials(tls_cert)
    auth_creds = grpc.metadata_call_credentials(metadata_callback)
    composite_creds = grpc.composite_channel_credentials(ssl_creds, auth_creds)

    target = f"{config.host}:{config.port}"
    try:
        return grpc.secure_channel(target, composite_creds)
    except Exception as exc:
        print_error(f"Failed to create gRPC channel: {exc}")
        return None


def get_lnd_stub(channel: grpc.Channel) -> lightning_pb2_grpc.LightningStub:
    """Return a Lightning stub for core LND RPCs."""
    return lightning_pb2_grpc.LightningStub(channel)


# ------------------------------------------------------------------------------
# LND RPC helpers
# ------------------------------------------------------------------------------

def get_info(lightning_stub: lightning_pb2_grpc.LightningStub) -> Optional[Tuple[str, str]]:
    """
    Fetch node info (pubkey and alias). Returns None on error.
    """
    try:
        resp = lightning_stub.GetInfo(lightning_pb2.GetInfoRequest())
        return resp.identity_pubkey, resp.alias
    except grpc.RpcError as exc:
        print_error(f"GetInfo failed: {exc}")
        return None


def get_forward_fees(
    lightning_stub: lightning_pb2_grpc.LightningStub, start_ts: int, end_ts: int
) -> Tuple[int, Dict[int, Dict[str, int]]]:
    """
    Sum forwarding fees in the given window.
    Returns total fees and a per-outbound-channel breakdown.
    """
    total_fees_sat = 0
    per_channel: Dict[int, Dict[str, int]] = defaultdict(lambda: {"fees_sat": 0, "out_volume_sat": 0})

    index_offset = 0
    page_size = 50_000

    while True:
        try:
            req = lightning_pb2.ForwardingHistoryRequest(
                start_time=start_ts,
                end_time=end_ts,
                index_offset=index_offset,
                num_max_events=page_size,
            )
            resp = lightning_stub.ForwardingHistory(req)
        except grpc.RpcError as exc:
            print_error(f"ForwardingHistory failed: {exc}")
            break

        events = list(resp.forwarding_events)
        if not events:
            break

        for event in events:
            fee_sat = _extract_fee_sat_from_event(event)
            amt_out_sat = _extract_amt_out_sat_from_event(event)
            total_fees_sat += fee_sat

            chan_id_out = getattr(event, "chan_id_out", 0)
            per_channel[chan_id_out]["fees_sat"] += fee_sat
            per_channel[chan_id_out]["out_volume_sat"] += amt_out_sat

        # Pagination control; stop if no progress is made.
        next_offset = getattr(resp, "last_offset_index", index_offset)
        if next_offset <= index_offset:
            break
        index_offset = next_offset

        if len(events) < page_size:
            break

    return total_fees_sat, per_channel


def _extract_fee_sat_from_event(event: lightning_pb2.ForwardingEvent) -> int:
    """Extract fee in satoshis from a forwarding event."""
    if hasattr(event, "fee"):
        return int(event.fee)
    if hasattr(event, "fee_msat"):
        return int(event.fee_msat // 1000)
    return 0


def _extract_amt_out_sat_from_event(event: lightning_pb2.ForwardingEvent) -> int:
    """Extract outgoing amount in satoshis from a forwarding event."""
    if hasattr(event, "amt_out"):
        return int(event.amt_out)
    if hasattr(event, "amt_out_msat"):
        return int(event.amt_out_msat // 1000)
    return 0


def get_rebalance_fees(
    lightning_stub: lightning_pb2_grpc.LightningStub,
    our_pubkey: str,
    start_ts: int,
    end_ts: int,
) -> Tuple[int, List[RebalanceDetail]]:
    """
    Sum fees for payments that qualify as rebalances in the given window.
    """
    total_fee_sat = 0
    details: List[RebalanceDetail] = []
    decode_cache: Dict[str, Tuple[Optional[str], Optional[str]]] = {}

    index_offset = 0
    page_size = 500
    early_exit = False

    while True:
        req = lightning_pb2.ListPaymentsRequest()
        # Optional filters depending on proto version.
        if "include_incomplete" in req.DESCRIPTOR.fields_by_name:
            req.include_incomplete = False
        if "index_offset" in req.DESCRIPTOR.fields_by_name:
            req.index_offset = index_offset
        if "max_payments" in req.DESCRIPTOR.fields_by_name:
            req.max_payments = page_size
        if "reversed" in req.DESCRIPTOR.fields_by_name:
            req.reversed = True  # fetch newest first to allow early cutoff
        if "creation_date_start" in req.DESCRIPTOR.fields_by_name:
            req.creation_date_start = start_ts
        if "creation_date_end" in req.DESCRIPTOR.fields_by_name:
            req.creation_date_end = end_ts

        try:
            resp = lightning_stub.ListPayments(req)
        except grpc.RpcError as exc:
            print_error(f"ListPayments failed: {exc}")
            break

        payments = list(resp.payments)
        if not payments:
            break

        for payment in payments:
            timestamp = _extract_payment_timestamp(payment)
            if timestamp < start_ts and getattr(req, "reversed", False):
                early_exit = True
                continue
            if timestamp < start_ts or timestamp > end_ts:
                continue

            if not _payment_succeeded(payment):
                continue

            dest, description = _extract_destination_and_description(
                payment, lightning_stub, decode_cache
            )

            is_internal_flag = bool(getattr(payment, "is_internal", False))
            description_lower = (description or "").lower()

            is_rebalance = (
                is_internal_flag
                or (dest is not None and dest == our_pubkey)
                or ("rebalance" in description_lower)
            )

            if is_rebalance:
                fee_sat = _extract_payment_fee_sat(payment)
                amt_sat = _extract_payment_amount_sat(payment)
                hop_count = _extract_hop_count(payment)
                total_fee_sat += fee_sat
                details.append(
                    RebalanceDetail(
                        payment_hash=getattr(payment, "payment_hash", ""),
                        amount_sat=amt_sat,
                        fee_sat=fee_sat,
                        hop_count=hop_count,
                    )
                )

        next_offset = getattr(resp, "last_index_offset", index_offset + len(payments))
        if next_offset <= index_offset:
            break
        index_offset = next_offset

        if early_exit or len(payments) < page_size:
            break

    return total_fee_sat, details


def _extract_payment_timestamp(payment) -> int:
    """Extract payment creation time in seconds."""
    if hasattr(payment, "creation_date"):
        return int(payment.creation_date)
    if hasattr(payment, "creation_time_ns"):
        return int(payment.creation_time_ns // 1_000_000_000)
    return 0


def _payment_succeeded(payment) -> bool:
    """Check if payment status is SUCCEEDED."""
    status = getattr(payment, "status", 0)
    succeeded_value = getattr(lightning_pb2.Payment, "SUCCEEDED", 2)
    return status == succeeded_value or status == 2


def _extract_destination_and_description(
    payment, lightning_stub, decode_cache: Dict[str, Tuple[Optional[str], Optional[str]]]
) -> Tuple[Optional[str], Optional[str]]:
    """
    Attempt to determine destination pubkey and description for a payment.
    Decode the payment request when available, with caching to reduce RPC load.
    """
    dest: Optional[str] = None
    description: Optional[str] = None

    # Use route information from a successful HTLC if available.
    for htlc in getattr(payment, "htlcs", []):
        status = getattr(htlc, "status", None)
        succ_value = getattr(lightning_pb2.HTLCAttempt.HTLCStatus, "SUCCEEDED", 1)
        if status == succ_value or status == 1:
            if hasattr(htlc, "route") and getattr(htlc.route, "hops", None):
                dest = htlc.route.hops[-1].pub_key
            break

    # Fallback to path (legacy field).
    path = getattr(payment, "path", None)
    if path:
        dest = path[-1]

    payreq = getattr(payment, "payment_request", "")
    if payreq:
        if payreq in decode_cache:
            cached_dest, cached_desc = decode_cache[payreq]
            dest = dest or cached_dest
            description = cached_desc
        else:
            try:
                decoded = lightning_stub.DecodePayReq(
                    lightning_pb2.PayReqString(pay_req=payreq)
                )
                dest = dest or decoded.destination
                description = getattr(decoded, "description", None)
                decode_cache[payreq] = (decoded.destination, description)
            except grpc.RpcError:
                # Decode errors are non-fatal; just skip memo/destination enrichment.
                decode_cache[payreq] = (None, None)

    return dest, description


def _extract_payment_fee_sat(payment) -> int:
    """Extract fee in satoshis from a payment."""
    if hasattr(payment, "fee_sat"):
        return int(payment.fee_sat)
    if hasattr(payment, "fee_msat"):
        return int(payment.fee_msat // 1000)
    return 0


def _extract_payment_amount_sat(payment) -> int:
    """Extract amount in satoshis from a payment."""
    if hasattr(payment, "value_sat"):
        return int(payment.value_sat)
    if hasattr(payment, "value_msat"):
        return int(payment.value_msat // 1000)
    if hasattr(payment, "amt_msat"):
        return int(payment.amt_msat // 1000)
    if hasattr(payment, "amt_sat"):
        return int(payment.amt_sat)
    return 0


def _extract_hop_count(payment) -> int:
    """Return hop count for the first succeeded HTLC route if available."""
    for htlc in getattr(payment, "htlcs", []):
        status = getattr(htlc, "status", None)
        succ_value = getattr(lightning_pb2.HTLCAttempt.HTLCStatus, "SUCCEEDED", 1)
        if status == succ_value or status == 1:
            if hasattr(htlc, "route") and getattr(htlc.route, "hops", None):
                return len(htlc.route.hops)
    path = getattr(payment, "path", None)
    if path:
        return len(path)
    return 0


# ------------------------------------------------------------------------------
# Main flow
# ------------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    """Parse CLI arguments."""
    parser = argparse.ArgumentParser(description="Compute LND forwarding and rebalance fees.")
    parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)")
    parser.add_argument(
        "--tz",
        dest="tz_name",
        help="Optional IANA timezone name (defaults to TZ env or local).",
    )
    parser.add_argument(
        "--months",
        dest="months",
        type=int,
        default=3,
        help="Include current month plus this many previous months in monthly report (minimum 1 month total).",
    )
    parser.add_argument(
        "--db-path",
        dest="db_path",
        default="lnd_fees.sqlite",
        help="Path to SQLite database file for cached daily results.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_cfg = load_env_config()
    if env_cfg is None:
        sys.exit(1)

    # CLI timezone overrides env.
    if args.tz_name:
        env_cfg.tz_name = args.tz_name

    date_bounds = resolve_date_bounds(args, env_cfg.tz_name)
    if date_bounds is None:
        sys.exit(1)

    start_date, end_date, tzinfo = date_bounds
    conn = init_db(args.db_path)

    target_dates = iter_dates(start_date, end_date)
    missing_dates = [d for d in target_dates if not day_exists(conn, d.isoformat())]

    lightning_stub = None
    our_pubkey = ""
    alias = ""
    new_records: List[DailyFeeRecord] = []

    if missing_dates:
        channel = create_grpc_channel(env_cfg)
        if channel is None:
            sys.exit(1)

        lightning_stub = get_lnd_stub(channel)

        info = get_info(lightning_stub)
        if info is None:
            sys.exit(1)
        our_pubkey, alias = info

        for target_date in missing_dates:
            tr = get_time_range_for_date(target_date, env_cfg.tz_name)
            forward_fees, _ = get_forward_fees(
                lightning_stub, tr.start_ts_utc, tr.end_ts_utc
            )
            rebalance_fees, _ = get_rebalance_fees(
                lightning_stub, our_pubkey, tr.start_ts_utc, tr.end_ts_utc
            )
            net_profit = forward_fees - rebalance_fees
            record = DailyFeeRecord(
                date_str=target_date.isoformat(),
                forward_fees_sat=forward_fees,
                rebalance_fees_sat=rebalance_fees,
                net_profit_sat=net_profit,
                start_ts_utc=tr.start_ts_utc,
                end_ts_utc=tr.end_ts_utc,
            )
            insert_daily_fee(conn, record)
            new_records.append(record)

        conn.commit()

    months_total = max(args.months, 1)
    monthly = aggregate_monthly(conn, tzinfo, months_total)
    yesterday_local = datetime.now(tzinfo).date() - timedelta(days=1)
    dminus1_record = fetch_daily_row(conn, yesterday_local.isoformat())

    print_run_summary(
        new_records=new_records,
        start_date=start_date,
        end_date=end_date,
        tzinfo=tzinfo,
        db_path=args.db_path,
        months=months_total,
        monthly_rows=monthly,
        alias=alias,
        pubkey=our_pubkey,
        missing_count=len(missing_dates),
        processed_count=len(new_records),
        dminus1_record=dminus1_record,
    )


def print_run_summary(
    new_records: List[DailyFeeRecord],
    start_date: date,
    end_date: date,
    tzinfo: timezone,
    db_path: str,
    months: int,
    monthly_rows: List[Tuple[str, int, int, int]],
    alias: str,
    pubkey: str,
    missing_count: int,
    processed_count: int,
    dminus1_record: Optional[DailyFeeRecord],
) -> None:
    """Print summary of this run plus daily D-1 and monthly aggregation."""
    tz_label = tzinfo.tzname(datetime.now(tzinfo)) or ""
    print(f"Date window requested: {start_date.isoformat()} -> {end_date.isoformat()} (tz={tz_label})")
    print(f"Database: {db_path}")

    if processed_count == 0:
        if missing_count == 0:
            print("No new daily rows inserted (database already up to date through requested range).")
        else:
            print("No daily rows inserted.")
    else:
        print(f"Inserted {processed_count} daily row(s):")
        for rec in sorted(new_records, key=lambda r: r.date_str):
            print(
                f"  {rec.date_str}: forward={rec.forward_fees_sat} sats, "
                f"rebalance={rec.rebalance_fees_sat} sats, net={rec.net_profit_sat} sats"
            )

    if alias:
        print(f"Node alias: {alias}")
    if pubkey:
        print(f"Node pubkey: {pubkey}")

    print()
    print_daily_report(dminus1_record, tzinfo)
    print()
    print_monthly_report(monthly_rows, months)


def print_monthly_report(monthly_rows: List[Tuple[str, int, int, int]], months: int) -> None:
    """Print monthly revenue/cost/profit for current month, last month, and remaining months."""
    if not monthly_rows:
        print("No monthly data available.")
        return

    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    print(f"Monthly routing profit snapshot (as of {today_str}):")

    current = monthly_rows[0]
    print(
        f"  Current month ({current[0]}): revenue={current[1]} sats, "
        f"rebalance_cost={current[2]} sats, profit={current[3]} sats"
    )

    if len(monthly_rows) >= 2:
        last = monthly_rows[1]
        print(
            f"  Last month ({last[0]}): revenue={last[1]} sats, "
            f"rebalance_cost={last[2]} sats, profit={last[3]} sats"
        )

    if len(monthly_rows) > 2:
        print("  Previous months:")
        for month_key, revenue, cost, profit in monthly_rows[2:]:
            print(
                f"    {month_key}: revenue={revenue} sats, "
                f"rebalance_cost={cost} sats, profit={profit} sats"
            )


def print_daily_report(dminus1_record: Optional[DailyFeeRecord], tzinfo: timezone) -> None:
    """Print D-1 daily profit."""
    yesterday = datetime.now(tzinfo).date() - timedelta(days=1)
    if dminus1_record is None:
        print(f"D-1 ({yesterday.isoformat()}): no data stored.")
        return
    print("D-1 daily profit:")
    print(
        f"  {dminus1_record.date_str}: revenue={dminus1_record.forward_fees_sat} sats, "
        f"rebalance_cost={dminus1_record.rebalance_fees_sat} sats, "
        f"profit={dminus1_record.net_profit_sat} sats"
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_error("Interrupted by user.")
        sys.exit(1)
