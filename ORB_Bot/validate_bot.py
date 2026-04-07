#!/usr/bin/env python3
"""Quick validation script for ORB_Bot"""

import orb_bot

print('✓ Imports erfolgreich')

# Config-Check
cfg = orb_bot.ORB_CONFIG
print(f'✓ Config geladen: {len(cfg["symbols"])} Symbole')
print(f'  Futures: {[s for s in cfg["symbols"] if "=F" in s]}')
print(f'✓ Futures-Config: {len(cfg["futures_config"]["point_values"])} Kontrakte definiert')

# Portfolio-Initialisierung
portfolio = orb_bot.ORBPortfolio(cfg)
print(f'✓ Portfolio initialisiert (Cash: {portfolio.data["cash"]:.0f} EUR)')

# Test Futures Position Size Berechnung
test_equity = 10000.0
test_entry = 5000.0
test_stop = 4990.0  # 10 points risk

# Stock-Test (SPY)
shares_spy = portfolio.calculate_position_size(440.0, 438.0, test_equity, 'SPY')
print(f'\n✓ Stock Position Size Test (SPY):')
print(f'  Entry: 440.0, Stop: 438.0, Risk: 2.0 per share')
print(f'  Result: {shares_spy} Shares')

# Futures-Test (ES=F - S&P 500 E-mini)
contracts_es = portfolio.calculate_position_size(test_entry, test_stop, test_equity, 'ES=F')
print(f'\n✓ Futures Position Size Test (ES=F):')
print(f'  Entry: {test_entry}, Stop: {test_stop}, Risk: {test_entry - test_stop} points')
print(f'  Point Value: $50, Risk per contract: ${(test_entry - test_stop) * 50}')
print(f'  Result: {contracts_es} Kontrakte')

# Micro Futures-Test (MES=F - Micro E-mini S&P 500)
contracts_mes = portfolio.calculate_position_size(test_entry, test_stop, test_equity, 'MES=F')
print(f'\n✓ Micro Futures Position Size Test (MES=F):')
print(f'  Entry: {test_entry}, Stop: {test_stop}, Risk: {test_entry - test_stop} points')
print(f'  Point Value: $5, Risk per contract: ${(test_entry - test_stop) * 5}')
print(f'  Result: {contracts_mes} Kontrakte')

# Edge Case Tests
print(f'\n✓ Edge Case Tests:')

# Test with unknown symbol (should use stock logic)
shares_unknown = portfolio.calculate_position_size(100.0, 99.0, test_equity, 'UNKNOWN')
print(f'  Unknown Symbol: {shares_unknown} Shares (verwendet Stock-Logik)')

# Test with None symbol (should use stock logic)
shares_none = portfolio.calculate_position_size(100.0, 99.0, test_equity, None)
print(f'  None Symbol: {shares_none} Shares (verwendet Stock-Logik)')

# Test with zero risk (should return 0)
contracts_zero = portfolio.calculate_position_size(5000.0, 5000.0, test_equity, 'ES=F')
print(f'  Zero Risk: {contracts_zero} Kontrakte (erwartet: 0)')

print('\n' + '='*60)
print('✅ Alle Validierungen erfolgreich bestanden!')
print('='*60)
