"""
Lightning Network daily fee report helper.

This script connects to an LND node via gRPC, fetches forwarding events and
outgoing payments, and computes routing income (forwards) versus costs
(rebalancing). The default time window is the previous local calendar day
("D-1"), but custom dates can be provided via CLI flags.

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
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
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
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    env_cfg = load_env_config()
    if env_cfg is None:
        sys.exit(1)

    # CLI timezone overrides env.
    if args.tz_name:
        env_cfg.tz_name = args.tz_name

    if args.from_date and not args.to_date or args.to_date and not args.from_date:
        print_error("Both --from and --to must be provided together.")
        sys.exit(1)

    if args.from_date and args.to_date:
        time_range = get_time_range_from_cli(args.from_date, args.to_date, env_cfg.tz_name)
    else:
        time_range = get_time_range_dminus1(env_cfg.tz_name)

    if time_range is None:
        sys.exit(1)

    channel = create_grpc_channel(env_cfg)
    if channel is None:
        sys.exit(1)

    lightning_stub = get_lnd_stub(channel)

    info = get_info(lightning_stub)
    if info is None:
        sys.exit(1)
    our_pubkey, alias = info

    total_forward_fees, per_channel = get_forward_fees(
        lightning_stub, time_range.start_ts_utc, time_range.end_ts_utc
    )

    total_rebalance_fees, rebalance_details = get_rebalance_fees(
        lightning_stub, our_pubkey, time_range.start_ts_utc, time_range.end_ts_utc
    )

    net_profit = total_forward_fees - total_rebalance_fees

    print_report(
        alias=alias,
        pubkey=our_pubkey,
        time_range=time_range,
        forward_fees=total_forward_fees,
        rebalance_fees=total_rebalance_fees,
        net_profit=net_profit,
        per_channel=per_channel,
        rebalance_count=len(rebalance_details),
    )


def print_report(
    alias: str,
    pubkey: str,
    time_range: TimeRange,
    forward_fees: int,
    rebalance_fees: int,
    net_profit: int,
    per_channel: Dict[int, Dict[str, int]],
    rebalance_count: int,
) -> None:
    """Print a human-readable summary to stdout."""
    start_local_str = time_range.start_local.strftime("%Y-%m-%d %H:%M:%S %Z")
    end_local_str = time_range.end_local.strftime("%Y-%m-%d %H:%M:%S %Z")
    start_utc_str = datetime.fromtimestamp(time_range.start_ts_utc, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )
    end_utc_str = datetime.fromtimestamp(time_range.end_ts_utc, tz=timezone.utc).strftime(
        "%Y-%m-%d %H:%M:%S UTC"
    )

    default_range_label = "custom range"
    today_local = datetime.now(time_range.start_local.tzinfo).date()
    yesterday = today_local - timedelta(days=1)
    if (
        time_range.start_local.date() == yesterday
        and time_range.end_local.date() == yesterday
    ):
        default_range_label = "D-1"

    print(f"Lightning daily fee report ({default_range_label})")
    print(f"Node alias: {alias}")
    print(f"Node pubkey: {pubkey}")
    print(f"Date range (local): {start_local_str} -> {end_local_str}")
    print(f"Date range (UTC):   {start_utc_str} -> {end_utc_str}")
    print()
    print("Forwarding fee income:")
    print(f"  Total forward fees: {forward_fees} sats")
    print()
    print("Rebalancing fee costs:")
    print(f"  Total rebalance fees: {rebalance_fees} sats")
    print(f"  Rebalance payments counted: {rebalance_count}")
    print()
    print("Net routing profit (forwards - rebalances):")
    print(f"  {net_profit} sats")

    if per_channel:
        print()
        print("Per-channel outbound breakdown (chan_id, out_volume_sat, fees_sat):")
        for chan_id, stats in sorted(per_channel.items(), key=lambda kv: kv[1]["fees_sat"], reverse=True):
            print(
                f"  {chan_id}: volume_out={stats['out_volume_sat']} sats, "
                f"fees={stats['fees_sat']} sats"
            )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print_error("Interrupted by user.")
        sys.exit(1)
