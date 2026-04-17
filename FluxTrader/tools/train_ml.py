#!/usr/bin/env python3
"""ML-Model-Training auf historischen Trade-Daten.

Standalone-Script – kein Import von live/ oder execution/.
Trainiert auf FeatureVector-Features aller Strategien.

Usage:
    python tools/train_ml.py --history trading_data/trades.db --output tools/models/
    python tools/train_ml.py --history trading_data/trades.db --output tools/models/ --model lgbm
    python tools/train_ml.py --help
"""
from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path

FEATURE_COLS = [
    "sma_diff", "adx", "atr_pct", "rsi", "macd_hist", "z_score", "volume_ratio",
]


def load_trades(db_path: Path) -> "pd.DataFrame":
    """Lade Trades aus der aiosqlite-DB (synchron, kein Event-Loop nötig)."""
    import pandas as pd
    conn = sqlite3.connect(str(db_path))
    try:
        df = pd.read_sql_query("SELECT * FROM trades", conn)
    finally:
        conn.close()

    if df.empty:
        print(f"[WARN] Keine Trades in {db_path}")
        return df

    print(f"[INFO] {len(df)} Trades geladen aus {db_path}")
    return df


def extract_features(df: "pd.DataFrame") -> tuple["np.ndarray", "np.ndarray"]:
    """Extrahiere Feature-Matrix X und Label-Vektor y aus Trade-DataFrame.

    Label: 1 wenn pnl > 0 (profitabler Trade), 0 sonst.
    Features kommen aus metadata-JSON oder direkt aus Spalten.
    """
    import numpy as np
    import pandas as pd
    records: list[dict] = []
    labels: list[int] = []

    for _, row in df.iterrows():
        pnl = float(row.get("pnl", 0.0))
        labels.append(1 if pnl > 0 else 0)

        # Features aus metadata (JSON) oder Spalten
        meta = {}
        if "metadata" in row and row["metadata"]:
            import json
            try:
                meta = json.loads(row["metadata"]) if isinstance(row["metadata"], str) else {}
            except (json.JSONDecodeError, TypeError):
                meta = {}

        records.append({
            "sma_diff": float(meta.get("sma_diff", 0.0)),
            "adx": float(meta.get("adx", 0.0)),
            "atr_pct": float(meta.get("atr_pct", 0.0)),
            "rsi": float(meta.get("rsi", 0.0)),
            "macd_hist": float(meta.get("macd_hist", 0.0)),
            "z_score": float(meta.get("z_score", 0.0)),
            "volume_ratio": float(meta.get("volume_ratio", 1.0)),
        })

    feat_df = pd.DataFrame(records, columns=FEATURE_COLS)
    return feat_df.values, np.array(labels)


def train_logistic(X_train, y_train):
    from sklearn.linear_model import LogisticRegression
    model = LogisticRegression(max_iter=1000, random_state=42)
    model.fit(X_train, y_train)
    return model


def train_lgbm(X_train, y_train):
    try:
        import lightgbm as lgb
    except ImportError:
        print("[ERROR] lightgbm nicht installiert. pip install lightgbm")
        sys.exit(1)

    model = lgb.LGBMClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.05,
        random_state=42,
        verbose=-1,
    )
    model.fit(X_train, y_train)
    return model


def main() -> None:
    parser = argparse.ArgumentParser(
        description="FluxTrader ML-Model-Training",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Beispiele:
  python tools/train_ml.py --history fluxtrader_data/state.db --output tools/models/
  python tools/train_ml.py --history fluxtrader_data/state.db --output tools/models/ --model lgbm
        """,
    )
    parser.add_argument("--history", "-H", required=True,
                        help="Pfad zur SQLite-DB mit Trade-Historie")
    parser.add_argument("--output", "-o", default="tools/models/",
                        help="Ausgabe-Verzeichnis für model.pkl + scaler.pkl")
    parser.add_argument("--model", "-m", default="logistic",
                        choices=["logistic", "lgbm"],
                        help="Modell-Typ (default: logistic)")
    parser.add_argument("--test-size", type=float, default=0.2,
                        help="Anteil Test-Daten (default: 0.2)")
    args = parser.parse_args()

    # Schwere Dependencies erst nach argparse laden
    import joblib
    import numpy as np
    from sklearn.metrics import accuracy_score, classification_report, roc_auc_score
    from sklearn.model_selection import train_test_split
    from sklearn.preprocessing import StandardScaler

    db_path = Path(args.history)
    if not db_path.exists():
        print(f"[ERROR] DB nicht gefunden: {db_path}")
        sys.exit(1)

    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Daten laden
    df = load_trades(db_path)
    if df.empty or len(df) < 10:
        print("[ERROR] Zu wenig Trades für Training (min. 10)")
        sys.exit(1)

    X, y = extract_features(df)
    print(f"[INFO] Features: {X.shape}, Labels: {y.shape}")
    print(f"[INFO] Label-Verteilung: positiv={y.sum()}, negativ={len(y) - y.sum()}")

    # Train/Test Split
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=args.test_size, random_state=42, stratify=y,
    )

    # Scaler
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Training
    print(f"\n[INFO] Training mit Modell: {args.model}")
    if args.model == "lgbm":
        model = train_lgbm(X_train_scaled, y_train)
    else:
        model = train_logistic(X_train_scaled, y_train)

    # Evaluation
    y_pred = model.predict(X_test_scaled)
    y_proba = model.predict_proba(X_test_scaled)[:, 1]

    print(f"\n[RESULT] Accuracy:  {accuracy_score(y_test, y_pred):.3f}")
    try:
        print(f"[RESULT] ROC-AUC:   {roc_auc_score(y_test, y_proba):.3f}")
    except ValueError:
        print("[RESULT] ROC-AUC:   n/a (nur eine Klasse im Test-Set)")
    print(f"\n{classification_report(y_test, y_pred)}")

    # Speichern
    joblib.dump(model, output_dir / "model.pkl")
    joblib.dump(scaler, output_dir / "scaler.pkl")
    print(f"[INFO] Modell gespeichert in {output_dir}")


if __name__ == "__main__":
    main()
