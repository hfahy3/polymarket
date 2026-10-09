"""Entry point.  python run.py          (reads .env)
MODE=mock  -> offline demo.   MODE=live -> real connection (PM_DRY_RUN=1 sends nothing).
python run.py verify          -> print proto message fields and check names."""
import asyncio
import logging
import os
import sys
from decimal import Decimal


def load_env(path=".env"):
    if not os.path.exists(path):
        return
    for line in open(path):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, v = line.split("=", 1)
            os.environ.setdefault(k.strip(), v.strip().strip('"').strip("'"))


async def main():
    load_env()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    from combo_mm import Config, Engine, MockClient
    e = os.environ.get
    cfg = Config(
        account=e("PM_ACCOUNT", "firm/account"),
        half_spread=Decimal(e("MM_HALF_SPREAD", "0.02")),
        per_leg_edge=Decimal(e("MM_PER_LEG_EDGE", "0.005")),
        max_legs=int(e("MM_MAX_LEGS", "6")),
        max_notional_per_rfq=Decimal(e("MM_MAX_NOTIONAL_PER_RFQ", "100")),
        max_open_exposure=Decimal(e("MM_MAX_OPEN_EXPOSURE", "500")),
    )
    mode = e("MODE", "mock")
    if mode == "mock":
        client = MockClient()
    else:
        from polymarket_us_client import RealClient, Settings
        client = RealClient(Settings.from_env())
        if len(sys.argv) > 1 and sys.argv[1] == "verify":
            sys.exit(0 if client.verify() else 1)
        logging.info("LIVE connection, dry_run=%s", client.s.dry_run)
    try:
        await Engine(client, cfg).run()
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    asyncio.run(main())
