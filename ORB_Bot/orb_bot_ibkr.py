"""
orb_bot_ibkr.py – Interactive Brokers Client für den ORB Bot.
Implementiert BrokerBase via ib_insync.

Voraussetzungen:
    pip install ib_insync pandas numpy pytz
    TWS oder IB Gateway muss lokal laufen.

Umgebungsvariablen:
    IBKR_HOST        – TWS/Gateway Host (Standard: 192.168.188.93)
    IBKR_PORT        – Port: 4002 (Gateway Paper) oder 4001 (Gateway Live), Standard: 4002
    IBKR_CLIENT_ID   – Eindeutige Client-ID (Standard: 1)
    IBKR_PAPER       – "true"/"false" (Standard: true)
    IBKR_BOT_ID      – Eindeutiger Bot-Name für Multi-Instanz-Betrieb (Standard: ORB)
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

from orb_broker_base import BrokerBase

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
                          overlay_reason: str = "", prefix: str = "") -> str:
    """
    Baut einen orderRef-String für IBKR (max 50 Zeichen).
    Format: [PREFIX|]BOT_ID|SYMBOL|SIDE|MMDD-HHMM[|overlay_kurz]
    Ist der String zu lang, wird von rechts gekürzt.
    """
    now_et = datetime.now(ET)
    ts = now_et.strftime("%m%d-%H%M")
    base = f"{bot_id}|{symbol}|{side}|{ts}"
    if prefix:
        base = f"{prefix}|{base}"
    if overlay_reason:
        remaining = 50 - len(base) - 1  # -1 für Trennzeichen
        if remaining > 3:
            base = f"{base}|{overlay_reason[:remaining]}"
    return base[:50]


# ─────────────────────────────────────────────────────────────────────────────
# IBKRClient
# ─────────────────────────────────────────────────────────────────────────────

class IBKRClient(BrokerBase):
    """
    Interactive Brokers Client – implementiert BrokerBase via ib_insync.

    Multi-Bot-Isolation:
    - clientId: pro Instanz eindeutig (Orders werden automatisch gefiltert)
    - bot_id: Präfix in orderRef + lokales Positions-Register
    - Positions-Register: JSON-Datei pro bot_id zur Trennung der Positionen
    """

    def __init__(self, host: str = "192.168.188.93", port: int = 4002,
                 client_id: int = 1, paper: bool = True,
                 bot_id: str = "ORB", data_dir: Optional[Path] = None,
                 order_prefix: str = ""):
        if not IBKR_AVAILABLE:
            raise RuntimeError("ib_insync fehlt – pip install ib_insync")

        self._paper = paper
        self._host = host
        self._port = port
        self._client_id = client_id
        self._bot_id = bot_id.upper()[:8]
        self._order_prefix = order_prefix

        self._data_dir = Path(data_dir) if data_dir else (
            Path(__file__).parent / "orb_trading_data"
        )
        self._data_dir.mkdir(parents=True, exist_ok=True)
        self._pos_file = self._data_dir / f"ibkr_positions_{self._bot_id}.json"

        self.ib = IB()
        self._connect()

        # Positions-Register laden und gegen IBKR abgleichen
        self._local_positions: dict = self._load_position_registry()
        self._reconcile_registry()

    # ── Verbindungsmanagement ───────────────────────────────────────────────

    def _connect(self):
        """Verbindet mit TWS/Gateway. Retry-Logik: 3 Versuche à 5s."""
        for attempt in range(1, 4):
            try:
                self.ib.connect(
                    self._host, self._port,
                    clientId=self._client_id,
                    timeout=20,
                    readonly=False,
                )
                mode = "PAPER" if self._paper else "LIVE"
                print(f"[IBKR] Verbunden  Host={self._host}:{self._port}  "
                      f"ClientId={self._client_id}  BotId={self._bot_id}  "
                      f"Modus={mode}")
                return
            except Exception as e:
                err = str(e)
                # Error 326: clientId bereits belegt
                if "326" in err:
                    print(f"[IBKR] FEHLER: ClientId {self._client_id} bereits "
                          f"belegt. Jede Bot-Instanz braucht eine eindeutige "
                          f"clientId.", file=sys.stderr)
                    raise
                if attempt < 3:
                    wait = attempt * 5
                    print(f"[IBKR] Verbindungsversuch {attempt}/3 fehlgeschlagen: "
                          f"{e}  – Retry in {wait}s")
                    time_module.sleep(wait)
                else:
                    print(f"[IBKR] FEHLER: Kann nicht verbinden nach 3 Versuchen. "
                          f"Ist TWS/Gateway gestartet auf {self._host}:{self._port}?",
                          file=sys.stderr)
                    raise

    def _ensure_connected(self):
        """Vor jedem API-Call aufrufen – reconnect falls nötig."""
        if not self.ib.isConnected():
            print("[IBKR] Verbindung verloren – reconnect...")
            self._connect()

    # ── Property ────────────────────────────────────────────────────────────

    @property
    def paper(self) -> bool:
        return self._paper

    # ── Marktdaten ──────────────────────────────────────────────────────────

    def fetch_bars(self, symbol: str, days: int = 2) -> pd.DataFrame:
        """5-Minuten-Bars via reqHistoricalData."""
        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            # Puffer für Wochenenden/Feiertage
            duration = f"{days * 2} D"

            bars = self.ib.reqHistoricalData(
                contract,
                endDateTime="",
                durationStr=duration,
                barSizeSetting="5 mins",
                whatToShow="TRADES",
                useRTH=True,
                keepUpToDate=False,
            )
            if not bars:
                return pd.DataFrame()

            df = util.df(bars)
            df = self._normalize_bars(df)
            return df
        except Exception as e:
            print(f"[IBKR] fetch_bars({symbol}) Fehler: {e}", file=sys.stderr)
            return pd.DataFrame()

    def check_bar_freshness(self, df: pd.DataFrame,
                            max_delay_minutes: int = 20) -> bool:
        """True wenn letzter Bar frisch genug."""
        if df.empty:
            return False
        last_ts = df.index[-1]
        if last_ts.tzinfo is None:
            last_ts = last_ts.tz_localize(pytz.UTC)
        now_utc = datetime.now(pytz.UTC)
        diff = (now_utc - last_ts).total_seconds() / 60
        return diff <= max_delay_minutes

    def fetch_bars_bulk(self, symbols: List[str], start: str,
                        end: str) -> Dict[str, pd.DataFrame]:
        """
        Historische 5m-Bars für mehrere Symbole.
        IBKR-Limit: max 60 Requests/10 Min → sleep(2) zwischen Symbolen.
        Error 162 (Pacing Violation) wird mit Backoff behandelt.
        """
        self._ensure_connected()
        result: Dict[str, pd.DataFrame] = {}

        start_dt = pd.Timestamp(start, tz=ET)
        end_dt = pd.Timestamp(end, tz=ET)
        total_days = (end_dt - start_dt).days

        for i, symbol in enumerate(symbols):
            if i > 0:
                time_module.sleep(2)  # IBKR Pacing

            try:
                contract = Stock(symbol, "SMART", "USD")
                self.ib.qualifyContracts(contract)

                # IBKR erlaubt max ~365 Tage für 5-min Bars pro Request
                # Bei längeren Zeiträumen in Chunks aufteilen
                all_bars = []
                chunk_start = start_dt
                while chunk_start < end_dt:
                    chunk_end = min(chunk_start + timedelta(days=365), end_dt)
                    chunk_days = (chunk_end - chunk_start).days + 1

                    for retry in range(3):
                        try:
                            bars = self.ib.reqHistoricalData(
                                contract,
                                endDateTime=chunk_end.strftime("%Y%m%d %H:%M:%S"),
                                durationStr=f"{chunk_days} D",
                                barSizeSetting="5 mins",
                                whatToShow="TRADES",
                                useRTH=True,
                                keepUpToDate=False,
                            )
                            if bars:
                                all_bars.extend(bars)
                            break
                        except Exception as e:
                            if "162" in str(e) or "pacing" in str(e).lower():
                                wait = 5 * (retry + 1)
                                print(f"[IBKR] Pacing Violation {symbol} – "
                                      f"warte {wait}s (Retry {retry+1}/3)")
                                time_module.sleep(wait)
                            else:
                                print(f"[IBKR] fetch_bars_bulk({symbol}) "
                                      f"Fehler: {e}", file=sys.stderr)
                                break

                    chunk_start = chunk_end
                    if chunk_start < end_dt:
                        time_module.sleep(2)

                if all_bars:
                    df = util.df(all_bars)
                    df = self._normalize_bars(df)
                    # Auf Zeitraum begrenzen
                    mask = (df.index >= start_dt.tz_convert(pytz.UTC)) & \
                           (df.index <= end_dt.tz_convert(pytz.UTC))
                    df = df.loc[mask]
                    if not df.empty:
                        result[symbol] = df

                print(f"[IBKR] {symbol}: {len(result.get(symbol, []))} Bars "
                      f"({i+1}/{len(symbols)})")

            except Exception as e:
                print(f"[IBKR] fetch_bars_bulk({symbol}) Fehler: {e}",
                      file=sys.stderr)

        return result

    @staticmethod
    def _normalize_bars(df: pd.DataFrame) -> pd.DataFrame:
        """Bringt IBKR-Bars auf das Standard-Format: Open/High/Low/Close/Volume, UTC-Index."""
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

        # DatetimeIndex setzen
        if "date" in df.columns:
            df = df.set_index("date")
        elif "Date" in df.columns:
            df = df.set_index("Date")

        if df.index.tzinfo is None:
            df.index = df.index.tz_localize(pytz.UTC)
        else:
            df.index = df.index.tz_convert(pytz.UTC)

        # Nur relevante Spalten behalten
        keep = [c for c in ["Open", "High", "Low", "Close", "Volume"]
                if c in df.columns]
        return df[keep]

    # ── Kontoinformationen ──────────────────────────────────────────────────

    def get_equity(self) -> float:
        """Aktueller Konto-Equity-Wert (NetLiquidation)."""
        self._ensure_connected()
        try:
            values = self.ib.accountValues()
            for v in values:
                if v.tag == "NetLiquidation" and v.currency == "USD":
                    return float(v.value)
            return 0.0
        except Exception as e:
            print(f"[IBKR] get_equity() Fehler: {e}", file=sys.stderr)
            return 0.0

    def get_cash(self) -> float:
        """Verfügbares Cash."""
        self._ensure_connected()
        try:
            values = self.ib.accountValues()
            for v in values:
                if v.tag == "TotalCashValue" and v.currency == "USD":
                    return float(v.value)
            return 0.0
        except Exception as e:
            print(f"[IBKR] get_cash() Fehler: {e}", file=sys.stderr)
            return 0.0

    def get_buying_power(self) -> float:
        """Kaufkraft."""
        self._ensure_connected()
        try:
            values = self.ib.accountValues()
            for v in values:
                if v.tag == "BuyingPower" and v.currency == "USD":
                    return float(v.value)
            return 0.0
        except Exception as e:
            print(f"[IBKR] get_buying_power() Fehler: {e}", file=sys.stderr)
            return 0.0

    # ── Positionen & Orders ─────────────────────────────────────────────────

    def sync_positions(self) -> Dict[str, dict]:
        """
        Positionen gefiltert nach lokalem Registry.
        Nur Symbole zurückgeben, die im eigenen Register stehen.
        """
        self._ensure_connected()
        try:
            ibkr_positions = self.ib.portfolio()

            # IBKR-Positionen als dict aufbereiten
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

            # Nur Symbole aus eigenem Register zurückgeben
            result: Dict[str, dict] = {}
            stale_symbols = []

            for sym, reg_data in list(self._local_positions.items()):
                if sym in ibkr_map:
                    pos = ibkr_map[sym]
                    # Entry aus Register übernehmen (genauer als IBKR avgCost
                    # bei Teilfüllungen)
                    pos["entry"] = reg_data.get("entry", pos["entry"])
                    result[sym] = pos
                else:
                    # Position im Register aber nicht mehr bei IBKR
                    # → wurde extern geschlossen (Stop/TP Fill)
                    print(f"[IBKR] {sym} nicht mehr bei IBKR – "
                          f"entferne aus Registry (war {reg_data.get('side', '?')})")
                    stale_symbols.append(sym)

            for sym in stale_symbols:
                self._unregister_position(sym)

            return result

        except Exception as e:
            print(f"[IBKR] sync_positions() Fehler: {e}", file=sys.stderr)
            return {}

    def is_shortable(self, symbol: str) -> bool:
        """Short-Verfügbarkeit prüfen. 5s Timeout, False bei Fehler."""
        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            # reqShortableShares gibt ein Ticker-Objekt zurück
            ticker = self.ib.reqMktData(contract, genericTickList="236",
                                        snapshot=False, regulatorySnapshot=False)
            # Warten auf shortableShares-Update (max 5s)
            for _ in range(10):
                self.ib.sleep(0.5)
                if ticker.shortableShares is not None and \
                   not np.isnan(ticker.shortableShares):
                    shares = ticker.shortableShares
                    self.ib.cancelMktData(contract)
                    return shares >= 1000  # konservativer Threshold

            self.ib.cancelMktData(contract)
            print(f"[IBKR] is_shortable({symbol}) Timeout – "
                  f"konservativ als nicht-shortbar gewertet")
            return False

        except Exception as e:
            print(f"[IBKR] is_shortable({symbol}) Fehler: {e}", file=sys.stderr)
            return False

    def get_open_orders(self) -> List[dict]:
        """Offene Orders (automatisch nach clientId gefiltert durch IBKR)."""
        self._ensure_connected()
        try:
            trades = self.ib.openTrades()
            orders = []
            for trade in trades:
                order = trade.order
                contract = trade.contract
                orders.append({
                    "id": str(order.orderId),
                    "symbol": contract.symbol,
                    "side": order.action.lower(),
                    "qty": int(order.totalQuantity),
                    "status": trade.orderStatus.status,
                })
            return orders
        except Exception as e:
            print(f"[IBKR] get_open_orders() Fehler: {e}", file=sys.stderr)
            return []

    # ── Order-Execution ─────────────────────────────────────────────────────

    def place_long_bracket(self, symbol: str, qty: int, stop_loss: float,
                           take_profit: float,
                           client_order_id: str = None) -> dict:
        """
        Long-Entry als Bracket-Order:
        Parent (Market BUY) + Stop-Loss (Stop SELL) + Take-Profit (Limit SELL)
        verknüpft via OCA-Gruppe.
        """
        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            order_ref = _build_ibkr_order_ref(
                self._bot_id, symbol, "BUY",
                client_order_id or "", self._order_prefix
            )

            # Parent Order – Market Buy
            parent = MarketOrder("BUY", int(qty))
            parent.orderId = self.ib.client.getReqId()
            parent.transmit = False
            parent.orderRef = order_ref

            oca_group = f"ORB_OCA_{parent.orderId}"

            # Stop-Loss Order – Stop Sell
            stop = StopOrder("SELL", int(qty), round(stop_loss, 2))
            stop.parentId = parent.orderId
            stop.orderId = self.ib.client.getReqId()
            stop.transmit = False
            stop.ocaGroup = oca_group
            stop.ocaType = 1  # Cancel verbleibende bei Ausführung
            stop.orderRef = order_ref

            # Take-Profit Order – Limit Sell (LETZTER → transmit=True)
            tp = LimitOrder("SELL", int(qty), round(take_profit, 2))
            tp.parentId = parent.orderId
            tp.orderId = self.ib.client.getReqId()
            tp.transmit = True  # Sendet alle 3 auf einmal
            tp.ocaGroup = oca_group
            tp.ocaType = 1
            tp.orderRef = order_ref

            # Alle 3 Orders platzieren
            parent_trade = self.ib.placeOrder(contract, parent)
            self.ib.placeOrder(contract, stop)
            self.ib.placeOrder(contract, tp)
            self.ib.sleep(1)

            # Position ins lokale Register eintragen
            self._register_position(
                symbol=symbol,
                parent_order_id=parent.orderId,
                side="long",
                qty=int(qty),
                entry=0.0,  # wird beim nächsten sync_positions aktualisiert
                stop_loss=stop_loss,
                take_profit=take_profit,
            )

            return {
                "ok": True,
                "id": str(parent.orderId),
                "symbol": symbol,
                "qty": int(qty),
                "side": "long",
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "status": parent_trade.orderStatus.status,
            }

        except Exception as e:
            err_msg = f"Long-Bracket {symbol}: {e}"
            print(f"[IBKR] FEHLER: {err_msg}", file=sys.stderr)
            return {"ok": False, "error": err_msg}

    def place_short_bracket(self, symbol: str, qty: int, stop_loss: float,
                            take_profit: float,
                            client_order_id: str = None) -> dict:
        """
        Short-Entry als Bracket-Order:
        Parent (Market SELL) + Stop-Loss (Stop BUY) + Take-Profit (Limit BUY)
        verknüpft via OCA-Gruppe.
        """
        # Short-Verfügbarkeit prüfen
        if not self.is_shortable(symbol):
            return {"ok": False, "error": f"{symbol} ist nicht shortbar"}

        self._ensure_connected()
        try:
            contract = Stock(symbol, "SMART", "USD")
            self.ib.qualifyContracts(contract)

            order_ref = _build_ibkr_order_ref(
                self._bot_id, symbol, "SHORT",
                client_order_id or "", self._order_prefix
            )

            # Parent Order – Market Sell (Short-Entry)
            parent = MarketOrder("SELL", int(qty))
            parent.orderId = self.ib.client.getReqId()
            parent.transmit = False
            parent.orderRef = order_ref

            oca_group = f"ORB_OCA_{parent.orderId}"

            # Stop-Loss Order – Stop Buy (Buy-to-Cover ÜBER Entry)
            stop = StopOrder("BUY", int(qty), round(stop_loss, 2))
            stop.parentId = parent.orderId
            stop.orderId = self.ib.client.getReqId()
            stop.transmit = False
            stop.ocaGroup = oca_group
            stop.ocaType = 1
            stop.orderRef = order_ref

            # Take-Profit Order – Limit Buy (Buy-to-Cover UNTER Entry)
            tp = LimitOrder("BUY", int(qty), round(take_profit, 2))
            tp.parentId = parent.orderId
            tp.orderId = self.ib.client.getReqId()
            tp.transmit = True  # Sendet alle 3
            tp.ocaGroup = oca_group
            tp.ocaType = 1
            tp.orderRef = order_ref

            # Alle 3 Orders platzieren
            parent_trade = self.ib.placeOrder(contract, parent)
            self.ib.placeOrder(contract, stop)
            self.ib.placeOrder(contract, tp)
            self.ib.sleep(1)

            # Position ins lokale Register eintragen
            self._register_position(
                symbol=symbol,
                parent_order_id=parent.orderId,
                side="short",
                qty=int(qty),
                entry=0.0,
                stop_loss=stop_loss,
                take_profit=take_profit,
            )

            return {
                "ok": True,
                "id": str(parent.orderId),
                "symbol": symbol,
                "qty": int(qty),
                "side": "short",
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "status": parent_trade.orderStatus.status,
            }

        except Exception as e:
            err_msg = f"Short-Bracket {symbol}: {e}"
            print(f"[IBKR] FEHLER: {err_msg}", file=sys.stderr)
            return {"ok": False, "error": err_msg}

    # ── Order-Management ────────────────────────────────────────────────────

    def cancel_all_orders(self) -> None:
        """
        Storniert NUR eigene Orders (gefiltert nach clientId durch IBKR).
        NIEMALS self.ib.reqGlobalCancel() verwenden – das würde alle Orders
        aller Bots auf dem Konto stornieren!
        """
        self._ensure_connected()
        try:
            open_trades = self.ib.openTrades()
            for trade in open_trades:
                try:
                    self.ib.cancelOrder(trade.order)
                except Exception as e:
                    print(f"[IBKR] Cancel-Fehler Order "
                          f"{trade.order.orderId}: {e}")
            if open_trades:
                self.ib.sleep(1)
                print(f"[IBKR] {len(open_trades)} Orders storniert "
                      f"(clientId={self._client_id})")
        except Exception as e:
            print(f"[IBKR] cancel_all_orders() Fehler: {e}", file=sys.stderr)

    def close_all_positions(self, verify: bool = True) -> dict:
        """
        EOD: NUR Positionen aus dem lokalen Register schließen.
        NICHT alle Kontopositionen – das würde Positionen anderer Bots schließen!
        """
        self._ensure_connected()
        attempted = []
        remaining = []

        # Erst offene Orders stornieren (eigene clientId)
        self.cancel_all_orders()

        # Positionen aus Register schließen
        for sym, reg_data in list(self._local_positions.items()):
            attempted.append(sym)
            try:
                contract = Stock(sym, "SMART", "USD")
                self.ib.qualifyContracts(contract)

                side = reg_data.get("side", "long")
                qty = int(reg_data.get("qty", 0))
                if qty <= 0:
                    self._unregister_position(sym)
                    continue

                # Gegenorder: Long → SELL, Short → BUY
                close_action = "SELL" if side == "long" else "BUY"
                close_order = MarketOrder(close_action, qty)
                close_order.orderRef = _build_ibkr_order_ref(
                    self._bot_id, sym, "CLOSE", "", self._order_prefix
                )

                self.ib.placeOrder(contract, close_order)
                print(f"[IBKR] Close {side} {sym} x{qty}")

            except Exception as e:
                print(f"[IBKR] Close-Fehler {sym}: {e}", file=sys.stderr)
                remaining.append(sym)

        if attempted:
            self.ib.sleep(3)

        # Verifikation
        if verify and attempted:
            ibkr_positions = self.ib.portfolio()
            ibkr_symbols = {
                item.contract.symbol
                for item in ibkr_positions
                if item.position != 0
            }
            for sym in list(self._local_positions.keys()):
                if sym in ibkr_symbols:
                    remaining.append(sym)
                    print(f"[IBKR] WARNUNG: {sym} noch offen nach Close-Versuch")
                else:
                    self._unregister_position(sym)

        ok = len(remaining) == 0
        return {"attempted": attempted, "remaining": remaining, "ok": ok}

    # ── Positions-Register ──────────────────────────────────────────────────

    def _load_position_registry(self) -> dict:
        """Lädt lokales Register aus JSON. Gibt {} zurück wenn Datei fehlt."""
        if not self._pos_file.exists():
            return {}
        try:
            data = json.loads(self._pos_file.read_text(encoding="utf-8"))
            return data if isinstance(data, dict) else {}
        except (json.JSONDecodeError, OSError) as e:
            print(f"[IBKR] Registry-Lesefehler {self._pos_file}: {e}",
                  file=sys.stderr)
            return {}

    def _save_position_registry(self) -> None:
        """Schreibt self._local_positions atomar in JSON (write temp + rename)."""
        try:
            self._data_dir.mkdir(parents=True, exist_ok=True)
            content = json.dumps(self._local_positions, indent=2, default=str)

            # Atomares Schreiben: temp-file im selben Verzeichnis + rename
            fd, tmp_path = tempfile.mkstemp(
                dir=str(self._data_dir), suffix=".tmp"
            )
            try:
                os.write(fd, content.encode("utf-8"))
                os.close(fd)
                # os.replace ist atomar auf den meisten Dateisystemen
                os.replace(tmp_path, str(self._pos_file))
            except Exception:
                os.close(fd) if not os.get_inheritable(fd) else None
                if os.path.exists(tmp_path):
                    os.unlink(tmp_path)
                raise
        except Exception as e:
            print(f"[IBKR] Registry-Schreibfehler: {e}", file=sys.stderr)

    def _register_position(self, symbol: str, parent_order_id: int,
                           side: str, qty: int,
                           entry: float, stop_loss: float,
                           take_profit: float) -> None:
        """Fügt Position nach erfolgreicher Order ins Register ein."""
        self._local_positions[symbol] = {
            "parent_order_id": parent_order_id,
            "side": side,
            "qty": qty,
            "entry": entry,
            "stop_loss": stop_loss,
            "take_profit": take_profit,
            "opened_at": datetime.now(pytz.UTC).isoformat(),
            "bot_id": self._bot_id,
        }
        self._save_position_registry()

    def _unregister_position(self, symbol: str) -> None:
        """Entfernt Position aus Register."""
        if symbol in self._local_positions:
            del self._local_positions[symbol]
            self._save_position_registry()

    def _reconcile_registry(self) -> None:
        """
        Startup-Check: Vergleicht lokales Register mit aktuellen IBKR-Positionen.
        - Symbole im Register aber nicht bei IBKR → aus Register entfernen
        - Symbole bei IBKR aber nicht im Register → nur warnen (gehören anderem Bot)
        """
        if not self._local_positions:
            return

        try:
            ibkr_positions = self.ib.portfolio()
            ibkr_symbols = {
                item.contract.symbol
                for item in ibkr_positions
                if item.position != 0
            }

            # Register-Einträge ohne IBKR-Pendant entfernen
            stale = [
                sym for sym in self._local_positions
                if sym not in ibkr_symbols
            ]
            for sym in stale:
                print(f"[IBKR] Reconcile: {sym} nicht mehr bei IBKR – "
                      f"entferne aus Registry")
                del self._local_positions[sym]

            if stale:
                self._save_position_registry()

            # IBKR-Positionen ohne Registry-Eintrag → Info (gehören anderem Bot)
            registered = set(self._local_positions.keys())
            unregistered = ibkr_symbols - registered
            if unregistered:
                print(f"[IBKR] Reconcile: IBKR-Positionen ohne Registry-Eintrag "
                      f"(gehören anderem Bot?): {sorted(unregistered)}")

            if self._local_positions:
                print(f"[IBKR] Registry geladen: "
                      f"{list(self._local_positions.keys())} "
                      f"(BotId={self._bot_id})")

        except Exception as e:
            print(f"[IBKR] Reconcile-Fehler: {e}", file=sys.stderr)

    # ── Disconnect ──────────────────────────────────────────────────────────

    def disconnect(self):
        """Verbindung zu TWS/Gateway trennen."""
        if self.ib.isConnected():
            self.ib.disconnect()
            print(f"[IBKR] Verbindung getrennt (ClientId={self._client_id})")
