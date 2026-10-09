"""
Polymarket US combo RFQ market-maker scaffold.

STATUS: skeleton. Runs end-to-end against MockClient (python combo_mm.py).
Live trading needs RealClient filled in from the Polymarket US proto bundle
(combo.proto, rfq.proto) and your institutional credentials.

Flow implemented (per docs.polymarket.us/trader-guide/combos):
  startup: GetRFQUserID -> StreamRFQEvents -> GetRFQs(OPEN) -> GetQuotes(SELF)
  rfq_created   -> price -> CreateQuote (two-sided, tick-aligned decimal strings)
  quote_accepted-> last look: re-price, then ConfirmQuote or DeleteQuote
                   before confirmationDeadline
  reconnect     -> repeat the durable reads (stream does not replay)
Fills: reconcile from Drop Copy, not from this loop.
"""
from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from decimal import Decimal, ROUND_DOWN, ROUND_UP
from typing import AsyncIterator, Optional, Protocol

log = logging.getLogger("combo_mm")


# --------------------------------------------------------------------------
# Config / risk
# --------------------------------------------------------------------------
@dataclass
class Config:
    account: str = "firm/account"            # fully qualified maker account
    half_spread: Decimal = Decimal("0.02")   # base half-spread around fair value
    per_leg_edge: Decimal = Decimal("0.005") # extra edge per leg (correlation / model risk)
    corr_haircut: Decimal = Decimal("0.01")  # extra half-spread if legs look correlated
    max_legs: int = 6
    max_notional_per_rfq: Decimal = Decimal("500")
    max_open_exposure: Decimal = Decimal("5000")  # gross, across all combos
    post_only: bool = True
    rest_remainder: bool = False
    last_look_margin_s: float = 0.75         # decide this long before the deadline
    last_look_max_drift: Decimal = Decimal("0.01")  # decline if fair moved > this vs quote


# --------------------------------------------------------------------------
# Data model (mirrors documented JSON field names, lower camel case)
# --------------------------------------------------------------------------
@dataclass
class Leg:
    symbol: str
    side: str                       # SIDE_BUY | SIDE_SELL
    settlementPrice: Optional[str] = None


@dataclass
class RFQ:
    id: str
    symbol: str                     # caoc-... combo symbol
    legs: list[Leg]
    qtyDecimal: Optional[str] = None
    cashOrderQty: Optional[str] = None
    tickSize: Optional[str] = None


@dataclass
class LiveQuote:
    quote_id: str
    rfq_id: str
    buy_price: Decimal
    sell_price: Decimal
    fair_at_quote: Decimal


# --------------------------------------------------------------------------
# Transport interface. Implement RealClient with the generated gRPC stubs.
# --------------------------------------------------------------------------
class Client(Protocol):
    async def get_rfq_user_id(self) -> str: ...
    def stream_rfq_events(self) -> AsyncIterator[dict]: ...
    async def get_open_rfqs(self) -> list[RFQ]: ...
    async def get_my_quotes(self) -> list[dict]: ...
    async def get_combo_tick(self, symbol: str) -> str: ...
    async def create_quote(self, rfq_id: str, buy: str, sell: str, *,
                           account: str, rest_remainder: bool, post_only: bool) -> str: ...
    async def confirm_quote(self, quote_id: str, rfq_id: str) -> None: ...
    async def delete_quote(self, quote_id: str, rfq_id: str) -> None: ...
    async def leg_mid(self, symbol: str) -> Optional[Decimal]: ...


class RealClient:
    """TODO: wire to polymarket.v1.ComboAPI / RFQAPI.

    - Base URLs: preprod https://api.preprod.polymarketexchange.com
                 prod    https://api.prod.polymarketexchange.com
    - Auth: bearer token + x-participant-id (or participant_id claim);
            scopes read:orders / write:orders.
    - StreamRFQEvents is gRPC-only. Generate stubs from rfq.proto.
    - leg_mid: take from the Market Data Stream / Order Book API.
    I have not verified REST paths or message schemas for the RFQ endpoints,
    so use the proto bundle as the source of truth rather than guessing here.
    """
    def __init__(self, *a, **k):
        raise NotImplementedError("Fill in from the Polymarket US proto bundle")


# --------------------------------------------------------------------------
# Pricing
# --------------------------------------------------------------------------
def _leg_prob(leg: Leg, mid: Decimal) -> Decimal:
    """Probability that this leg's chosen side wins. SIDE_SELL means the combo
    needs the leg's YES to NOT happen. Assumption: confirm against combo docs."""
    if leg.settlementPrice is not None:
        yes = Decimal(leg.settlementPrice)
    else:
        yes = mid
    return yes if leg.side == "SIDE_BUY" else Decimal(1) - yes


async def fair_value(client: Client, rfq: RFQ) -> Optional[Decimal]:
    """Independence baseline: product of leg probabilities.
    REPLACE with a correlation-aware model for same-event legs."""
    p = Decimal(1)
    for leg in rfq.legs:
        mid = None
        if leg.settlementPrice is None:
            mid = await client.leg_mid(leg.symbol)
            if mid is None:
                return None  # no market data -> do not quote
        p *= _leg_prob(leg, mid if mid is not None else Decimal(0))
    return p


def looks_correlated(rfq: RFQ) -> bool:
    """Crude placeholder: legs sharing a market-symbol prefix (same event)."""
    prefixes = [l.symbol.rsplit("-", 1)[0] for l in rfq.legs]
    return len(set(prefixes)) < len(prefixes)


def align(price: Decimal, tick: Decimal, *, up: bool) -> Decimal:
    q = (price / tick).to_integral_value(rounding=ROUND_UP if up else ROUND_DOWN)
    return q * tick


def build_quote(cfg: Config, rfq: RFQ, fair: Decimal) -> Optional[tuple[Decimal, Decimal]]:
    tick = Decimal(rfq.tickSize or "0.001")
    half = cfg.half_spread + cfg.per_leg_edge * len(rfq.legs)
    if looks_correlated(rfq):
        half += cfg.corr_haircut
    ask = align(fair + half, tick, up=True)     # buyPrice: requester buys, we sell
    bid = align(fair - half, tick, up=False)    # sellPrice: requester sells, we buy
    ask = min(ask, Decimal(1) - tick)
    bid = max(bid, Decimal(0))
    if ask <= 0 or ask <= bid:
        return None
    return bid, ask


def rfq_notional(rfq: RFQ, price: Decimal) -> Decimal:
    if rfq.cashOrderQty is not None:
        return Decimal(rfq.cashOrderQty)
    return Decimal(rfq.qtyDecimal or "0") * price


# --------------------------------------------------------------------------
# Engine
# --------------------------------------------------------------------------
class Engine:
    def __init__(self, client: Client, cfg: Config):
        self.c, self.cfg = client, cfg
        self.rfqs: dict[str, RFQ] = {}
        self.quotes: dict[str, LiveQuote] = {}        # quote_id -> quote
        self.closed_rfqs: set[str] = set()
        self.exposure = Decimal(0)

    async def recover(self):
        """Durable reads. Run at startup and after every reconnect."""
        for r in await self.c.get_open_rfqs():
            self.rfqs[r.id] = r
        my = await self.c.get_my_quotes()
        log.info("recovered %d open RFQs, %d own quotes", len(self.rfqs), len(my))
        # TODO: rebuild self.quotes / exposure from `my` and from positions API.

    async def on_rfq_created(self, rfq: RFQ):
        self.rfqs[rfq.id] = rfq
        if len(rfq.legs) > self.cfg.max_legs:
            return
        fair = await fair_value(self.c, rfq)
        if fair is None:
            return
        px = build_quote(self.cfg, rfq, fair)
        if px is None:
            return
        bid, ask = px
        if rfq_notional(rfq, ask) > self.cfg.max_notional_per_rfq:
            log.info("skip %s: notional too large", rfq.id)
            return
        if self.exposure + rfq_notional(rfq, ask) > self.cfg.max_open_exposure:
            log.info("skip %s: exposure cap", rfq.id)
            return
        qid = await self.c.create_quote(
            rfq.id, str(ask), str(bid), account=self.cfg.account,
            rest_remainder=self.cfg.rest_remainder, post_only=self.cfg.post_only)
        self.quotes[qid] = LiveQuote(qid, rfq.id, buy_price=ask, sell_price=bid, fair_at_quote=fair)
        log.info("quoted %s bid=%s ask=%s fair=%.4f", rfq.id, bid, ask, fair)

    def on_rfq_closed(self, rfq_id: str):
        # Stop (re)quoting, but keep quote state: acceptance also closes the RFQ.
        self.closed_rfqs.add(rfq_id)

    async def on_quote_accepted(self, ev: dict):
        """Last look. Use the server deadline, never a local timer alone."""
        quote_id = ev["quote"]["id"]
        rfq_id = ev["quote"]["rfqId"]
        deadline = ev["confirmationDeadline"]          # epoch seconds (adapt to real type)
        lq = self.quotes.get(quote_id)
        rfq = self.rfqs.get(rfq_id)
        if lq is None or rfq is None:
            log.warning("accepted unknown quote %s; declining", quote_id)
            await self.c.delete_quote(quote_id, rfq_id)
            return
        accepted_side = ev.get("acceptedSide")  # None if the event does not say
        decision = await self._last_look_ok(lq, rfq, accepted_side, deadline)
        if decision:
            await self.c.confirm_quote(quote_id, rfq_id)
            self.exposure += rfq_notional(rfq, lq.buy_price)
            log.info("CONFIRMED %s", quote_id)
        else:
            await self.c.delete_quote(quote_id, rfq_id)
            log.info("DECLINED %s", quote_id)

    async def _last_look_ok(self, lq: LiveQuote, rfq: RFQ, side: Optional[str], deadline: float) -> bool:
        if time.time() > deadline - self.cfg.last_look_margin_s:
            return False                                # too late to act safely
        fair_now = await fair_value(self.c, rfq)
        if fair_now is None:
            return False
        if abs(fair_now - lq.fair_at_quote) > self.cfg.last_look_max_drift:
            return False
        # If the event does not say which side was accepted, require edge on every
        # side we quoted (conservative).
        sides = [side] if side else ["SIDE_BUY", "SIDE_SELL"]
        for s in sides:
            px = lq.buy_price if s == "SIDE_BUY" else lq.sell_price
            if px <= 0:
                continue
            edge = (px - fair_now) if s == "SIDE_BUY" else (fair_now - px)
            if edge <= 0:
                return False
        return True
        # TODO: hedge-cost check against leg order books before confirming.

    async def _guard(self, coro):
        try:
            await coro
        except Exception:
            log.exception("handler failed")

    async def run(self):
        uid = await self.c.get_rfq_user_id()
        log.info("rfq user id: %s", uid)
        while True:
            try:
                await self.recover()
                async for ev in self.c.stream_rfq_events():
                    t = ev["type"]
                    if t == "rfq_created":
                        asyncio.create_task(self._guard(self.on_rfq_created(ev["rfq"])))
                    elif t == "rfq_closed":
                        self.on_rfq_closed(ev["rfqId"])
                    elif t == "quote_accepted":
                        asyncio.create_task(self._guard(self.on_quote_accepted(ev)))
                    elif t in ("quote_confirmed", "quote_executed", "quote_deleted", "quote_created"):
                        log.info("event %s", t)         # state sync; fills come from Drop Copy
                    elif t == "rfq_trade":
                        pass                            # dedupe by trade_id if you track flow
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("stream dropped; recovering")
                await asyncio.sleep(1)


# --------------------------------------------------------------------------
# Mock client for dry runs
# --------------------------------------------------------------------------
class MockClient:
    def __init__(self):
        self.mids = {"nba-a-win": Decimal("0.60"), "nba-b-win": Decimal("0.45"),
                     "nfl-x-over": Decimal("0.52")}
        self._n = 0

    async def get_rfq_user_id(self): return "rfquser_mock"
    async def get_open_rfqs(self): return []
    async def get_my_quotes(self): return []
    async def get_combo_tick(self, symbol): return "0.001"
    async def leg_mid(self, symbol): return self.mids.get(symbol)

    async def create_quote(self, rfq_id, buy, sell, **kw):
        self._n += 1
        print(f"  [mock] CreateQuote rfq={rfq_id} buy={buy} sell={sell} {kw}")
        return f"quote_{self._n}"

    async def confirm_quote(self, quote_id, rfq_id): print(f"  [mock] ConfirmQuote {quote_id}")
    async def delete_quote(self, quote_id, rfq_id): print(f"  [mock] DeleteQuote {quote_id}")

    async def stream_rfq_events(self):
        rfq = RFQ("rfq_1", "caoc-demo",
                  [Leg("nba-a-win", "SIDE_BUY"), Leg("nfl-x-over", "SIDE_BUY")],
                  qtyDecimal="100", tickSize="0.001")
        yield {"type": "rfq_created", "rfq": rfq}
        await asyncio.sleep(0.3)
        yield {"type": "quote_accepted", "acceptedSide": "SIDE_BUY",
               "quote": {"id": "quote_1", "rfqId": "rfq_1"},
               "confirmationDeadline": time.time() + 3}
        await asyncio.sleep(0.3)
        raise asyncio.CancelledError


async def _demo():
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    try:
        await Engine(MockClient(), Config()).run()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(_demo())
