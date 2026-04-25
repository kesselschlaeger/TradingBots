# Botti Pair – SPY/QQQ Kalman Pair-Trading

Botti Pair ist eine **marktneutrale Pair-Trading-Strategie** auf Daily-Bars. Der
Spread zwischen zwei korrelierten Indizes (Default: SPY und QQQ) wird über einen
Kalman-Filter geglättet; ein Z-Score zeigt an, wann der Spread signifikant von seinem
historischen Mittelwert abweicht.

Die Kernidee: SPY und QQQ sind hochkorreliert, divergieren aber kurzfristig. Der
Kalman-Z-Score misst diese Divergenz adaptiv; bei extremen Abweichungen (|z| > 2.0)
wird der Underperformer long und der Outperformer short gegangen — in Erwartung
der Mean-Reversion.

Registriert als `"botti_pair"`.

---

## Funktionsprinzip

```mermaid
flowchart TD
    A[Neuer Daily-Bar von SPY + QQQ] --> B[Spread = Close_B − Close_A]
    B --> C[Kalman-Filter: z_score\naus geglättetem Spread]
    C --> D{|z| > z_entry?}
    D -- Nein --> E{|z| < z_exit?}
    E -- Ja --> F[EXIT-Signal\nbeiden Legs schließen]
    E -- Nein --> G[HOLD]
    D -- Ja --> H{z > 0?}
    H -- Ja --> I[ENTER: Long SPY / Short QQQ\nQQQ outperformt → SPY underperformend]
    H -- Nein --> J[ENTER: Long QQQ / Short SPY\nSPY outperformt → QQQ underperformend]
```

---

## Benötigte Daten

| Quelle | Timeframe | Wozu |
|---|---|---|
| **SPY** (symbol_a) | `1Day` | Spread-Berechnung, Long- oder Short-Leg |
| **QQQ** (symbol_b) | `1Day` | Spread-Berechnung, Long- oder Short-Leg |

Für Live: `provider: ibkr`, `timeframe: "1D"`, `lookback_days: 60`.
Für Backtest: `provider: yfinance`, `timeframe: "1D"`.

**Mindest-Warmup:** 3 Bars Spread-Window (`pair_lookback` Default: 20 Bars für stabile Std-Berechnung).

---

## Handelszeiten / Tagesablauf

Botti Pair läuft auf **Daily-Bars** — identisch zu Botti.

| Uhrzeit ET | Event |
|---|---|
| ~16:00–16:05 | Daily-Bar vollständig → PairEngine wertet Spread aus |
| ~16:05 | Daily-Summary per Telegram (wenn aktiviert) |
| Über Nacht | Beide Legs gehalten (Long + Short gleichzeitig) |

**Wichtig:**
- Beide Legs (Long + Short) werden **simultan** als Market-Orders platziert.
- Bei Failure eines Legs cancelt `execute_pair_signal` automatisch den anderen Leg
  (kein Leg-Mismatch / einseitiges Exposure).
- Beide Legs teilen dieselbe `reserve_group = "pair_SPY_QQQ"`.

---

## Signal-Logik

| Bedingung | Aktion |
|---|---|
| `z > z_entry` (z.B. > 2.0) | ENTER: Long `symbol_a` (SPY), Short `symbol_b` (QQQ) |
| `z < −z_entry` | ENTER: Long `symbol_b` (QQQ), Short `symbol_a` (SPY) |
| `|z| < z_exit` (z.B. < 0.5) | EXIT: beide Legs schließen |
| sonst | HOLD |

---

## Kalman-Filter

Der Kalman-Filter schätzt den "wahren" Spread adaptiv — resistenter gegen Ausreißer
als ein einfaches Rolling-Mean:

```
Prozessrauschen  q: 1e-5  (niedriger = träger Filter, langsamere Anpassung)
Messrauschen     r: 0.01  (höher = mehr Glättung)
```

Der Z-Score berechnet sich aus dem gefilterten Spread und der Rolling-Standardabweichung
des Spread-Windows (Standard: 20 Tage).

---

## Sizing

```
qty_per_leg = equity × pair_position_pct  (Default: 5% je Leg)
```

Beide Legs bekommen dieselbe Equity-Quote. Kein ATR-Sizing, kein Stop-Loss —
der Market-neutrale Ansatz begrenzt das Risiko durch die gegenläufige Position.

---

## Parameter-Referenz

| Parameter | Default | Beschreibung |
|---|---|---|
| `symbol_a` | `"SPY"` | Erstes Symbol (Long oder Short je nach z-Vorzeichen) |
| `symbol_b` | `"QQQ"` | Zweites Symbol (gegenläufig zu symbol_a) |
| `z_entry` | `2.0` | Z-Score-Schwelle für Entry |
| `z_exit` | `0.5` | Z-Score-Schwelle für Exit (Mean-Reversion-Ziel) |
| `pair_position_pct` | `0.05` | Equity-Anteil je Leg (5%) |
| `pair_lookback` | `20` | Rolling-Window für Spread-Standardabweichung (Tage) |
| `kalman_q` | `1e-5` | Kalman-Prozessrauschen (träger = kleineres q) |
| `kalman_r` | `0.01` | Kalman-Messrauschen (mehr Glättung = größeres r) |

---

## Konfiguration

```yaml
strategy:
  name: botti_pair
  symbols: [SPY, QQQ]
  params:
    symbol_a: SPY
    symbol_b: QQQ
    z_entry: 2.0
    z_exit: 0.5
    pair_position_pct: 0.05
    pair_lookback: 20
    kalman_q: 0.00001
    kalman_r: 0.01

data:
  provider: ibkr
  timeframe: "1D"
  lookback_days: 60
```

```bash
python main.py paper --config configs/botti_pair.yaml
python main.py live  --config configs/botti_pair.yaml
```

---

## Implementierung

```
strategy/botti_pair.py
  BottiPairStrategy._generate_pair_signal(bar_a, bar_b, snapshot)
    ├─ spread = bar_b.close − bar_a.close
    ├─ _kalman.z_score(spread, rolling_std)  → z
    ├─ z > z_entry  → PairSignal(ENTER, long_symbol=SPY, short_symbol=QQQ)
    ├─ z < −z_entry → PairSignal(ENTER, long_symbol=QQQ, short_symbol=SPY)
    ├─ |z| < z_exit → PairSignal(EXIT)
    └─ sonst        → PairSignal(HOLD, qty_pct=0)

execution/port.py
  execute_pair_signal(signal, equity)
    → submit_order(long_leg) + submit_order(short_leg)
    → bei Failure: cancel(long_leg) → kein einseitiges Exposure

live/pair_runner.py → PairEngine (asyncio Task)
live/state.py → beide Legs via register_and_persist (gemeinsame reserve_group)
```

!!! note "Erweiterbarkeit"
    Andere Paare (z.B. GLD/SLV, XLE/XOM) können über `symbol_a`/`symbol_b` konfiguriert
    werden. Die Strategie-Klasse ist nicht auf SPY/QQQ beschränkt.
