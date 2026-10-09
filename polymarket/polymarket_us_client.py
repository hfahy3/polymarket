"""
Real client for Polymarket US (polymarket.v1.RFQAPI / ComboAPI over gRPC).

READ THIS FIRST - what is verified and what is not
  Verified from docs.polymarket.us: service names, RPC names (GetRFQUserID,
  StreamRFQEvents, GetRFQs, GetQuotes, CreateQuote, ConfirmQuote, DeleteQuote,
  GetCombos), JSON field names for RFQ/quote bodies, bearer-token auth with an
  x-participant-id header, scopes read:orders / write:orders.
  NOT verified (I could not see the proto bundle or onboarding material):
    - the gRPC host:port                      -> PM_GRPC_TARGET
    - how key + secret become a bearer token  -> fetch_token()
    - exact generated module / message names  -> run RealClient.verify()
    - stream event wrapper shape              -> normalize_event()
    - order book REST path and response shape -> PM_BOOK_URL_TEMPLATE / _best_bid_ask()
  Everything above is isolated so you can fix it in one place each.
  With PM_DRY_RUN=1 (default) no quote/confirm/delete is ever sent.
"""
from __future__ import annotations

import asyncio
import importlib
import json
import logging
import os
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import AsyncIterator, Optional

from combo_mm import RFQ, Leg

log = logging.getLogger("pmus")


# --------------------------------------------------------------------------
# Settings: this is where your key, id and secret go (via .env)
# --------------------------------------------------------------------------
@dataclass
class Settings:
    api_key: str = ""
    api_secret: str = ""
    participant_id: str = ""
    account: str = ""
    bearer_token: str = ""
    grpc_target: str = ""
    use_tls: bool = True
    book_url_template: str = ""
    dry_run: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        e = os.environ.get
        return cls(
            api_key=e("PM_API_KEY", ""),
            api_secret=e("PM_API_SECRET", ""),
            participant_id=e("PM_PARTICIPANT_ID", ""),
            account=e("PM_ACCOUNT", ""),
            bearer_token=e("PM_BEARER_TOKEN", ""),
            grpc_target=e("PM_GRPC_TARGET", ""),
            use_tls=e("PM_USE_TLS", "1") != "0",
            book_url_template=e("PM_BOOK_URL_TEMPLATE", ""),
            dry_run=e("PM_DRY_RUN", "1") != "0",
        )

    def check(self):
        missing = []
        if not self.participant_id:
            missing.append("PM_PARTICIPANT_ID")
        if not self.grpc_target:
            missing.append("PM_GRPC_TARGET")
        if not self.bearer_token and not (self.api_key and self.api_secret):
            missing.append("PM_BEARER_TOKEN or (PM_API_KEY + PM_API_SECRET)")
        if not self.account:
            missing.append("PM_ACCOUNT")
        if missing:
            raise SystemExit("Missing settings in .env: " + ", ".join(missing))


# --------------------------------------------------------------------------
# Generated-module names. Change here if your bundle differs.
# --------------------------------------------------------------------------
RFQ_PB, RFQ_GRPC = "rfq_pb2", "rfq_pb2_grpc"
COMBO_PB, COMBO_GRPC = "combo_pb2", "combo_pb2_grpc"
RFQ_STUB, COMBO_STUB = "RFQAPIStub", "ComboAPIStub"
# RPC -> (module key, request message). Convention assumed: <Rpc>Request.
RPCS = {
    "GetRFQUserID": "rfq", "StreamRFQEvents": "rfq", "GetRFQs": "rfq",
    "GetQuotes": "rfq", "CreateQuote": "rfq", "ConfirmQuote": "rfq",
    "DeleteQuote": "rfq", "GetCombos": "combo",
}


# --------------------------------------------------------------------------
# Auth
# --------------------------------------------------------------------------
async def fetch_token(api_key: str, api_secret: str) -> str:
    """TODO: exchange key + secret for a bearer token.
    I did not find the exchange documented publicly. Ask Polymarket how key/secret
    become a token (OAuth client-credentials? signed request?) and implement it
    here. Until then set PM_BEARER_TOKEN directly."""
    raise NotImplementedError(
        "Implement fetch_token() per Polymarket's auth docs, or set PM_BEARER_TOKEN.")


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------
def to_epoch(v) -> float:
    """Protobuf Timestamp JSON (RFC3339), epoch seconds, or epoch millis -> seconds."""
    if v is None:
        raise ValueError("missing deadline")
    if isinstance(v, (int, float)):
        return v / 1000.0 if v > 1e12 else float(v)
    if isinstance(v, dict) and "seconds" in v:
        return float(v["seconds"]) + float(v.get("nanos", 0)) / 1e9
    try:
        f = float(v)
        return f / 1000.0 if f > 1e12 else f
    except (TypeError, ValueError):
        return datetime.fromisoformat(str(v).replace("Z", "+00:00")).timestamp()


def parse_rfq(d: dict) -> RFQ:
    legs = [Leg(l["symbol"], l.get("side", "SIDE_BUY"), l.get("settlementPrice"))
            for l in d.get("comboLegs", [])]
    return RFQ(id=d["id"], symbol=d.get("symbol", ""), legs=legs,
               qtyDecimal=d.get("qtyDecimal"), cashOrderQty=d.get("cashOrderQty"),
               tickSize=d.get("tickSize"))


_EVENT_NAMES = {
    "rfqCreated": "rfq_created", "rfqClosed": "rfq_closed",
    "quoteCreated": "quote_created", "quoteDeleted": "quote_deleted",
    "quoteAccepted": "quote_accepted", "quoteConfirmed": "quote_confirmed",
    "quoteExecuted": "quote_executed", "rfqTrade": "rfq_trade",
    "rfq_created": "rfq_created", "rfq_closed": "rfq_closed",
    "quote_created": "quote_created", "quote_deleted": "quote_deleted",
    "quote_accepted": "quote_accepted", "quote_confirmed": "quote_confirmed",
    "quote_executed": "quote_executed", "rfq_trade": "rfq_trade",
}


def normalize_event(d: dict) -> Optional[dict]:
    """ADAPT: map a stream message (as camelCase dict) to the engine's event dicts.
    Tolerates a oneof wrapper ({"rfqCreated": {...}}) or a {"type": ..., ...} shape."""
    name, payload = None, d
    if isinstance(d.get("type"), str) and d["type"] in _EVENT_NAMES:
        name = _EVENT_NAMES[d["type"]]
    else:
        for k, v in d.items():
            if k in _EVENT_NAMES:
                name, payload = _EVENT_NAMES[k], (v or {})
                break
            if isinstance(v, dict):                   # one more level, e.g. {"event": {"rfqCreated": ..}}
                for k2, v2 in v.items():
                    if k2 in _EVENT_NAMES:
                        name, payload = _EVENT_NAMES[k2], (v2 or {})
                        break
                if name:
                    break
    if name is None:
        log.debug("unrecognised stream message: %s", list(d.keys()))
        return None

    if name == "rfq_created":
        return {"type": name, "rfq": parse_rfq(payload.get("rfq", payload))}
    if name == "rfq_closed":
        rid = payload.get("rfqId") or payload.get("rfq", {}).get("id") or payload.get("id")
        return {"type": name, "rfqId": rid}
    if name == "quote_accepted":
        q = payload.get("quote", {})
        deadline = payload.get("confirmationDeadline") or q.get("confirmationDeadline")
        return {"type": name, "quote": {"id": q.get("id"), "rfqId": q.get("rfqId")},
                "acceptedSide": payload.get("acceptedSide") or q.get("acceptedSide"),
                "confirmationDeadline": to_epoch(deadline)}
    return {"type": name, "raw": payload}


def _best_bid_ask(data: dict) -> tuple[Optional[Decimal], Optional[Decimal]]:
    """ADAPT to the order book response shape."""
    def num(x):
        try:
            return Decimal(str(x["price"] if isinstance(x, dict) else x))
        except Exception:
            return None
    bid = data.get("bestBid") or (data.get("bids") or [None])[0]
    ask = data.get("bestAsk") or (data.get("asks") or [None])[0]
    return (num(bid) if bid is not None else None, num(ask) if ask is not None else None)


# --------------------------------------------------------------------------
# Client
# --------------------------------------------------------------------------
class RealClient:
    def __init__(self, s: Settings):
        s.check()
        self.s = s
        import grpc  # imported late so mock mode needs no grpc install
        self._grpc = grpc
        creds = grpc.ssl_channel_credentials() if s.use_tls else None
        self._channel = (grpc.aio.secure_channel(s.grpc_target, creds) if s.use_tls
                         else grpc.aio.insecure_channel(s.grpc_target))
        self._mods = {}
        for key, pb, gr in (("rfq", RFQ_PB, RFQ_GRPC), ("combo", COMBO_PB, COMBO_GRPC)):
            try:
                self._mods[key] = (importlib.import_module(pb), importlib.import_module(gr))
            except ImportError as e:
                raise SystemExit(f"Missing generated module for {key}: {e}. Run ./gen_protos.sh "
                                 "with Polymarket's .proto files in ./protos") from e
        self._rfq_stub = getattr(self._mods["rfq"][1], RFQ_STUB)(self._channel)
        self._combo_stub = getattr(self._mods["combo"][1], COMBO_STUB)(self._channel)
        self._quote_rfq: dict[str, str] = {}
        self._book_cache: dict[str, tuple[float, Optional[Decimal]]] = {}
        self._token: str = s.bearer_token
        self._n = 0

    # ---- plumbing ----
    async def _metadata(self):
        if not self._token:
            self._token = await fetch_token(self.s.api_key, self.s.api_secret)
        return (("authorization", f"Bearer {self._token}"),
                ("x-participant-id", self.s.participant_id))

    def _msg(self, rpc: str, body: dict):
        from google.protobuf.json_format import ParseDict
        pb = self._mods[RPCS[rpc]][0]
        cls = getattr(pb, f"{rpc}Request")
        return ParseDict(body, cls(), ignore_unknown_fields=True)

    async def _call(self, rpc: str, body: dict) -> dict:
        from google.protobuf.json_format import MessageToDict
        stub = self._rfq_stub if RPCS[rpc] == "rfq" else self._combo_stub
        resp = await getattr(stub, rpc)(self._msg(rpc, body), metadata=await self._metadata())
        return MessageToDict(resp)

    def verify(self):
        """Print each request message's fields so you can check the JSON bodies below
        match the real protos (ParseDict silently drops unknown fields)."""
        ok = True
        for rpc, key in RPCS.items():
            cls = getattr(self._mods[key][0], f"{rpc}Request", None)
            if cls is None:
                print(f"MISSING request class {rpc}Request in {key} module")
                ok = False
            else:
                print(f"{rpc}Request fields: {[f.name for f in cls.DESCRIPTOR.fields]}")
        for stub_name, obj in ((RFQ_STUB, self._rfq_stub), (COMBO_STUB, self._combo_stub)):
            print(f"{stub_name} methods: {[m for m in dir(obj) if m[0].isupper()]}")
        return ok

    # ---- Client protocol used by the engine ----
    async def get_rfq_user_id(self) -> str:
        r = await self._call("GetRFQUserID", {})
        return next((v for v in r.values() if isinstance(v, str)), "")

    async def get_open_rfqs(self) -> list[RFQ]:
        r = await self._call("GetRFQs", {"status": "RFQ_STATUS_OPEN"})
        # TODO: follow pagination cursors if the response carries one.
        return [parse_rfq(x) for x in r.get("rfqs", [])]

    async def get_my_quotes(self) -> list[dict]:
        r = await self._call("GetQuotes", {"userFilter": "USER_FILTER_SELF"})
        return r.get("quotes", [])

    async def get_combo_tick(self, symbol: str) -> str:
        r = await self._call("GetCombos", {"symbol": symbol})
        combos = r.get("combos", [])
        return (combos[0].get("tickSize") if combos else None) or "0.001"

    async def create_quote(self, rfq_id, buy, sell, *, account, rest_remainder, post_only) -> str:
        account = account or self.s.account
        body = {"rfqId": rfq_id, "buyPrice": buy, "sellPrice": sell,
                "restRemainder": rest_remainder, "postOnly": post_only, "account": account}
        if self.s.dry_run:
            self._n += 1
            qid = f"dry_{self._n}"
            log.info("[DRY RUN] CreateQuote %s", body)
        else:
            r = await self._call("CreateQuote", body)
            qid = r.get("quoteId") or r.get("quote", {}).get("id")
            if not qid:
                raise RuntimeError(f"CreateQuote returned no quote id: {r}")
        self._quote_rfq[qid] = rfq_id
        return qid

    async def confirm_quote(self, quote_id: str, rfq_id: str) -> None:
        body = {"rfqId": rfq_id, "quoteId": quote_id}
        if self.s.dry_run:
            log.info("[DRY RUN] ConfirmQuote %s", body)
            return
        await self._call("ConfirmQuote", body)

    async def delete_quote(self, quote_id: str, rfq_id: str) -> None:
        body = {"rfqId": rfq_id, "quoteId": quote_id}
        if self.s.dry_run:
            log.info("[DRY RUN] DeleteQuote %s", body)
            return
        await self._call("DeleteQuote", body)

    async def stream_rfq_events(self) -> AsyncIterator[dict]:
        from google.protobuf.json_format import MessageToDict
        call = self._rfq_stub.StreamRFQEvents(self._msg("StreamRFQEvents", {}),
                                              metadata=await self._metadata())
        async for msg in call:
            ev = normalize_event(MessageToDict(msg))
            if ev:
                yield ev

    async def leg_mid(self, symbol: str) -> Optional[Decimal]:
        now = time.time()
        hit = self._book_cache.get(symbol)
        if hit and now - hit[0] < 0.25:
            return hit[1]
        if not self.s.book_url_template:
            raise RuntimeError("Set PM_BOOK_URL_TEMPLATE (order book URL with {symbol}).")
        await self._metadata()  # ensures a token is loaded
        url = self.s.book_url_template.format(symbol=symbol)
        data = await asyncio.to_thread(self._http_json, url)
        bid, ask = _best_bid_ask(data)
        mid = (bid + ask) / 2 if bid is not None and ask is not None else None
        self._book_cache[symbol] = (now, mid)
        return mid

    def _http_json(self, url: str) -> dict:
        req = urllib.request.Request(url, headers={
            "Authorization": f"Bearer {self._token}",
            "x-participant-id": self.s.participant_id})
        with urllib.request.urlopen(req, timeout=2) as r:
            return json.loads(r.read())
