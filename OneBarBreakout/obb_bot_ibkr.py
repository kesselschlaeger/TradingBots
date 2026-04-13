"""
obb_bot_ibkr.py – Interactive Brokers Client für den One-Bar-Breakout Bot.
Implementiert OBBBrokerBase via ib_insync für Daily-Bar-Strategien.

Voraussetzungen:
    pip install ib_insync pandas numpy pytz
    TWS oder IB Gateway muss laufen.

Umgebungsvariablen:
    IBKR_HOST        – TWS/Gateway Host (Standard: 192.168.188.93)
    IBKR_PORT        – Port: 4002 (Gateway Paper) oder 4001 (Gateway Live)
    IBKR_CLIENT_ID   – Eindeutige Client-ID (Standard: 1)
    IBKR_PAPER       – "true"/"false" (Standard: true)
    IBKR_BOT_ID      – Eindeutiger Bot-Name für Multi-Instanz-Betrieb (Standard: OBB)
"""

import json
import os
import sys
import tempfile
import time as time_module
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
import pytz

from obb_broker_base import OBBBrokerBase

try:
    from ib_insync import (
        IB, Contract, LimitOrder, MarketOrder, Stock, StopOrder, util,
    )
    IBKR_AVAILABLE = True
except ImportError:
    IBKR_AVAILABLE = False

ET = pytz.timezone("US/Eastern")


# ─────────────────────────────────────────────────────────────────────────────
# Hilfsfunktionen
# ─────────────────────────────────────────────────────────────────────────────

def _build_ibkr_order_ref(bot_id: str, symbol: str, side: str,
                          extra: str = "") -> str:
    """orderRef für IBKR (max 50 Zeichen)."""
    now_et = datetime.now(ET)
    ts = now_et.strftime("%m%d-%H%M")
    base = f"{bot_id}|{symbol}|{side}|{ts}"
    if extra:
        remaining = 50 - len(base) - 1
        if remaining > 3:
            base = f"{base}|{extra[:remaining]}"
    return base[:50]


# ─────────────────────────────────────────────────────────────────────────────
# IBKRClientDaily
# ─────────────────────────────────────────────────────────────────────────────

class IBKRClientDaily(OBBBrokerBase):
    """
    Interactive Brokers Client für Daily-Bar-Strategien (One-Bar-Breakout).

    Multi-Bot-Isolation:
    - clientId: pro Instanz eindeutig
    - bot_id: Präfix in orderRef + lokales Positions-Register
    - Positions-Register: JSON-Datei pro bot_id
    """

    def __init__(self, host: str = "192.168.188.93", port: int = 4002,
                 client_id: int = 1, paper: bool = True,
                 bot_id: str = "OBB", data_dir: Optional[Path] = None):
        if not IBKR_AVAILABLE:
            raise RuntimeError("ib_insync fehlt – pip install ib_insync")

        self._paper = paper
        self._host = host
        self._port = port
        self._client_id = client_id
        self._bot_id = bot_id.upper()[:8]

        self._data_dir = Path(data_dir) if data_dir else (
            Path(__file__).parent / "obb_trading_data"
        )
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._pos_file = self._data_dir / f"ibkr_positions_{self._bot_id}.json"

        self.ib = IB()
        self._connect()

        self._local_positions: dict = self._load_position_registry()
        self._reconcile_registry()

    # ── Verbindungsmanagement ───────────────────────────────────────────────

    def _connect(self):
        """Verbindet mit TWS/Gateway. 3 Versuche à 5s."""
        for attempt in range(1, 4):
            try:
                self.ib.connect(
                    self._host, self._port,
                    clientId=self._client_id,
                    timeout=20,
                    readonly=False,
                )
                mode = "PAPER" if self._paper else "LIVE"
                print(f"[IBKR-OBB] Verbunden  Host={self._host}:{self._port}  "
                      f"ClientId={self._client_id}  BotId={self._bot_id}  "
                      f"Modus={mode}")
                return
            except Exception as e:
                err = str(e)
                if "326" in err:
                    print(f"[IBKR-OBB] FEHLER: ClientId {self._client_id} bereits "
                          f"belegt.", file=sys.stderr)
                    raise
                if attempt < 3:
                    wait = attempt * 5
                    print(f"[IBKR-OBB] Verbindungsversuch {attempt}/3 fehlgeschlagen: "
                          f"{e}  – Retry in {wait}s")
                    time_module.sleep(wait)
                else:
                    print(f"[IBKR-OBB] FEHLER: Kann nicht verbinden. "
                          f"Ist TWS/Gateway gestartet auf {self._host}:{self._port}?",
                          file=sys.stderr)
                    raise

    def _ensure_connected(self):
        if not self.ib.isConnected():
            print("[IBKR-OBB] Verbindung verloren – reconnect...")
            self._connect()

    # ── Property ────────────────────────────────────────────────────────────

    @property
    def paper(self) -> bool:
        return self._paper

    # ── Marktdaten (Daily) ──────────────────────────────────────────────────

    def fetch_daily_bars(self, symbol: str, days: int = 80) -> pd.DataFrame:
        """Tägliche OHLCV-Bars via reqHistoricalData."""
        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            duration = f"{days * 2} D"  # Puffer für Wochenenden/Feiertage

            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting="1 day",
                whatToShow="TRADES",
                useRTH=True,
                keepUpToDate=False,
            )
            if not bars:
                return pd.DataFrame()

            df = util.df(bars)
            df = self._normalize_bars(df)
            return df.tail(days)
        except Exception as e:
            print(f"[IBKR-OBB] fetch_daily_bars({symbol}) Fehler: {e}",
                  file=sys.stderr)
            return pd.DataFrame()

    def fetch_daily_bars_bulk(self, symbols: List[str], start: str,
                              end: str) -> Dict[str, pd.DataFrame]:
        """
        Historische Daily-Bars für mehrere Symbole.
        IBKR-Limit: sleep(2) zwischen Symbolen, Error 162 Backoff.
        """
        self._ensure_connected()
        result: Dict[str, pd.DataFrame] = {}

        start_dt = pd.Timestamp(start, tz=ET)
        end_dt = pd.Timestamp(end, tz=ET)
        total_days = (end_dt - start_dt).days

        for i, symbol in enumerate(symbols):
            if i > 0:
                time_module.sleep(2)

            try:
                contract = Stock(symbol, "SMART", "USD")
                self.ib.qualifyContracts(contract)

                for retry in range(3):
                    try:
                        bars = self.ib.reqHistoricalData(
                            contract,
                            endDateTime=end_dt.strftime("%Y%m%d %H:%M:%S"),
                            durationStr=f"{total_days + 5} D",
                            barSizeSetting="1 day",
                            whatToShow="TRADES",
                            useRTH=True,
                            keepUpToDate=False,
                        )
                        if bars:
                            df = util.df(bars)
                            df = self._normalize_bars(df)
                            mask = (df.index >= start_dt.tz_convert(pytz.UTC)) & \
                                   (df.index <= end_dt.tz_convert(pytz.UTC))
                            df = df.loc[mask]
                            if not df.empty:
                                result[symbol] = df
                        break
                    except Exception as e:
                        if "162" in str(e) or "pacing" in str(e).lower():
                            wait = 5 * (retry + 1)
                            print(f"[IBKR-OBB] Pacing {symbol} – warte {wait}s")
                            time_module.sleep(wait)
                        else:
                            print(f"[IBKR-OBB] Bulk({symbol}) Fehler: {e}",
                                  file=sys.stderr)
                            break

                print(f"[IBKR-OBB] {symbol}: {len(result.get(symbol, []))} Bars "
                      f"({i+1}/{len(symbols)})")

            except Exception as e:
                print(f"[IBKR-OBB] Bulk({symbol}) Fehler: {e}", file=sys.stderr)

        return result

    @staticmethod
    def _normalize_bars(df: pd.DataFrame) -> pd.DataFrame:
        """Bringt IBKR-Bars auf Standard-Format."""
        rename_map = {}
        for col in df.columns:
            low = col.lower()
            if low == "open":
                rename_map[col] = "Open"
            elif low == "high":
                rename_map[col] = "High"
            elif low == "low":
                rename_map[col] = "Low"
            elif low == "close":
                rename_map[col] = "Close"
            elif low == "volume":
                rename_map[col] = "Volume"
        if rename_map:
            df = df.rename(columns=rename_map)

        if "date" in df.columns:
            df = df.set_index("date")
        elif "Date" in df.columns:
            df = df.set_index("Date")

        # Timezone-Handling für DatetimeIndex (nicht Timestamp)
        if hasattr(df.index, 'tz'):
            if df.index.tz is None:
                df.index = df.index.tz_localize(pytz.UTC)
            else:
                df.index = df.index.tz_convert(pytz.UTC)

        keep = [c for c in ["Open", "High", "Low", "Close", "Volume"]
                if c in df.columns]
        return df[keep]

    # ── Kontoinformationen ──────────────────────────────────────────────────

    def get_equity(self) -> float:
        self._ensure_connected()
        try:
            for v in self.ib.accountValues():
                if v.tag == "NetLiquidation":
                    return float(v.value)
            return 0.0
        except Exception as e:
            print(f"[IBKR-OBB] get_equity() Fehler: {e}", file=sys.stderr)
            return 0.0

    def get_cash(self) -> float:
        self._ensure_connected()
        try:
            for v in self.ib.accountValues():
                if v.tag == "TotalCashValue":
                    return float(v.value)
            return 0.0
        except Exception as e:
            print(f"[IBKR-OBB] get_cash() Fehler: {e}", file=sys.stderr)
            return 0.0

    def get_buying_power(self) -> float:
        self._ensure_connected()
        try:
            for v in self.ib.accountValues():
                if v.tag == "BuyingPower":
                    return float(v.value)
            return 0.0
        except Exception as e:
            print(f"[IBKR-OBB] get_buying_power() Fehler: {e}", file=sys.stderr)
            return 0.0
            return 0.0

    # ── Positionen & Orders ─────────────────────────────────────────────────

    def sync_positions(self) -> Dict[str, dict]:
        """Positionen gefiltert nach lokalem Registry."""
        self._ensure_connected()
        try:
            ibkr_positions = self.ib.portfolio()
            ibkr_map: Dict[str, dict] = {}
            for item in ibkr_positions:
                sym = item.contract.symbol
                qty = item.position
                if qty == 0:
                    continue
                ibkr_map[sym] = {
                    "qty": abs(qty),
                    "side": "long" if qty > 0 else "short",
                    "entry": item.averageCost,
                    "current_price": item.marketPrice or 0.0,
                    "unrealized_pnl": item.unrealizedPNL or 0.0,
                    "market_value": item.marketValue or 0.0,
                }

            result: Dict[str, dict] = {}
            stale = []
            for sym, reg_data in list(self._local_positions.items()):
                if sym in ibkr_map:
                    pos = ibkr_map[sym]
                    pos["entry"] = reg_data.get("entry", pos["entry"])
                    result[sym] = pos
                else:
                    print(f"[IBKR-OBB] {sym} nicht mehr bei IBKR – "
                          f"entferne aus Registry")
                    stale.append(sym)

            for sym in stale:
                self._unregister_position(sym)

            return result
        except Exception as e:
            print(f"[IBKR-OBB] sync_positions() Fehler: {e}", file=sys.stderr)
            return {}

    def is_shortable(self, symbol: str) -> bool:
        """Short-Verfügbarkeit. 5s Timeout, False bei Fehler."""
        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)
            ticker = self.ib.reqMktData(contract, genericTickList="236",
                                        snapshot=False, regulatorySnapshot=False)
            for _ in range(10):
                self.ib.sleep(0.5)
                if ticker.shortableShares is not None and \
                   not np.isnan(ticker.shortableShares):
                    shares = ticker.shortableShares
                    self.ib.cancelMktData(contract)
                    return shares >= 1000
            self.ib.cancelMktData(contract)
            return False
        except Exception as e:
            print(f"[IBKR-OBB] is_shortable({symbol}) Fehler: {e}",
                  file=sys.stderr)
            return False

    def get_open_orders(self) -> List[dict]:
        """Offene Orders (clientId-gefiltert durch IBKR)."""
        self._ensure_connected()
        try:
            trades = self.ib.openTrades()
            return [
                {
                    "id": str(t.order.orderId),
                    "symbol": t.contract.symbol,
                    "side": t.order.action.lower(),
                    "qty": int(t.order.totalQuantity),
                    "status": t.orderStatus.status,
                }
                for t in trades
            ]
        except Exception as e:
            print(f"[IBKR-OBB] get_open_orders() Fehler: {e}", file=sys.stderr)
            return []

    # ── Order-Execution ─────────────────────────────────────────────────────

    def place_market_order(self, symbol: str, qty: int, side: str,
                           time_in_force: str = "day",
                           client_order_id: str = "") -> dict:
        """
        Einfache Market-Order.

        IBKR-Hinweis zu time_in_force:
        - "day" → Standard-Market-Order
        - "opg" → Opening-Order (vor Marktöffnung einreichen)
        - "cls" → MOC (Market-on-Close), muss vor 15:50 ET eingereicht werden
        - "gtc" → Good-till-Cancel
        """
        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            action = "BUY" if side.lower() == "buy" else "SELL"
            order = MarketOrder(action, int(qty))

            # Time-in-Force Mapping
            tif_upper = time_in_force.upper()
            if tif_upper == "OPG":
                order.tif = "OPG"
            elif tif_upper in ("CLS", "MOC"):
                # IBKR: Market-on-Close = orderType="MOC"
                order.orderType = "MOC"
                order.tif = "DAY"
            elif tif_upper == "GTC":
                order.tif = "GTC"
            else:
                order.tif = "DAY"

            order_ref = _build_ibkr_order_ref(
                self._bot_id, symbol, action, client_order_id
            )
            order.orderRef = order_ref

            trade = self.ib.placeOrder(contract, order)
            self.ib.sleep(1)

            # Position ins Register eintragen (bei Entry-Orders)
            if action == "BUY" and side.lower() == "buy":
                self._register_position(symbol, "long", int(qty), 0.0)
            elif action == "SELL" and side.lower() == "sell":
                # Könnte Short-Entry oder Close-Long sein
                # Nur registrieren wenn kein Close (Position nicht im Register)
                if symbol not in self._local_positions:
                    self._register_position(symbol, "short", int(qty), 0.0)

            return {
                "ok": True,
                "id": str(order.orderId),
                "symbol": symbol,
                "qty": int(qty),
                "side": side,
                "status": trade.orderStatus.status,
            }
        except Exception as e:
            err_msg = f"Market-Order {symbol} {side}: {e}"
            print(f"[IBKR-OBB] FEHLER: {err_msg}", file=sys.stderr)
            return {"ok": False, "error": err_msg}

    # ── Positionsmanagement ─────────────────────────────────────────────────

    def cancel_all_orders(self) -> None:
        """
        Storniert NUR eigene Orders (clientId-gefiltert).
        NIEMALS self.ib.reqGlobalCancel()!
        """
        self._ensure_connected()
        try:
            open_trades = self.ib.openTrades()
            for trade in open_trades:
                try:
                    self.ib.cancelOrder(trade.order)
                except Exception as e:
                    print(f"[IBKR-OBB] Cancel-Fehler {trade.order.orderId}: {e}")
            if open_trades:
                self.ib.sleep(1)
                print(f"[IBKR-OBB] {len(open_trades)} Orders storniert")
        except Exception as e:
            print(f"[IBKR-OBB] cancel_all_orders() Fehler: {e}", file=sys.stderr)

    def close_position(self, symbol: str) -> dict:
        """Einzelne Position schließen (nur aus eigenem Register)."""
        self._ensure_connected()
        try:
            reg = self._local_positions.get(symbol)
            if not reg:
                # Fallback: direkt bei IBKR schauen
                for item in self.ib.portfolio():
                    if item.contract.symbol == symbol and item.position != 0:
                        reg = {
                            "side": "long" if item.position > 0 else "short",
                            "qty": abs(item.position),
                        }
                        break
                if not reg:
                    return {"ok": False, "error": f"{symbol} nicht gefunden"}

            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            side = reg.get("side", "long")
            qty = int(reg.get("qty", 0))
            close_action = "SELL" if side == "long" else "BUY"
            close_order = MarketOrder(close_action, qty)
            close_order.orderRef = _build_ibkr_order_ref(
                self._bot_id, symbol, "CLOSE"
            )

            self.ib.placeOrder(contract, close_order)
            self.ib.sleep(1)
            self._unregister_position(symbol)
            print(f"[IBKR-OBB] Position {symbol} geschlossen ({side} x{qty})")
            return {"ok": True}
        except Exception as e:
            print(f"[IBKR-OBB] close_position({symbol}) Fehler: {e}",
                  file=sys.stderr)
            return {"ok": False, "error": str(e)}

    def close_all_positions(self) -> dict:
        """NUR Positionen aus dem lokalen Register schließen."""
        self._ensure_connected()
        self.cancel_all_orders()

        closed = []
        for sym in list(self._local_positions.keys()):
            result = self.close_position(sym)
            if result.get("ok"):
                closed.append(sym)

        return {"ok": True, "closed": closed}

    # ── Positions-Register ──────────────────────────────────────────────────

    def _load_position_registry(self) -> dict:
        if not self._pos_file.exists():
            return {}
        try:
            data = json.loads(self._pos_file.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError) as e:
            print(f"[IBKR-OBB] Registry-Lesefehler: {e}", file=sys.stderr)
            return {}

    def _save_position_registry(self) -> None:
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            content = json.dumps(self._local_positions, indent=2, default=str)
            fd, tmp_path = tempfile.mkstemp(
                dir=str(self._data_dir), suffix=".tmp"
            )
            try:
                os.write(fd, content.encode("utf-8"))
                os.close(fd)
                os.replace(tmp_path, str(self._pos_file))
            except Exception:
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except Exception as e:
            print(f"[IBKR-OBB] Registry-Schreibfehler: {e}", file=sys.stderr)

    def _register_position(self, symbol: str, side: str, qty: int,
                           entry: float) -> None:
        self._local_positions[symbol] = {
            "side": side,
            "qty": qty,
            "entry": entry,
            "opened_at": datetime.now(pytz.UTC).isoformat(),
            "bot_id": self._bot_id,
        }
        self._save_position_registry()

    def _unregister_position(self, symbol: str) -> None:
        if symbol in self._local_positions:
            del self._local_positions[symbol]
            self._save_position_registry()

    def _reconcile_registry(self) -> None:
        if not self._local_positions:
            return
        try:
            ibkr_symbols = {
                item.contract.symbol
                for item in self.ib.portfolio()
                if item.position != 0
            }
            stale = [s for s in self._local_positions if s not in ibkr_symbols]
            for sym in stale:
                print(f"[IBKR-OBB] Reconcile: {sym} nicht mehr bei IBKR – entferne")
                del self._local_positions[sym]
            if stale:
                self._save_position_registry()

            unregistered = ibkr_symbols - set(self._local_positions.keys())
            if unregistered:
                print(f"[IBKR-OBB] Reconcile: IBKR-Positionen ohne Registry: "
                      f"{sorted(unregistered)}")

            if self._local_positions:
                print(f"[IBKR-OBB] Registry: {list(self._local_positions.keys())} "
                      f"(BotId={self._bot_id})")
        except Exception as e:
            print(f"[IBKR-OBB] Reconcile-Fehler: {e}", file=sys.stderr)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()
            print(f"[IBKR-OBB] Verbindung getrennt (ClientId={self._client_id})")
