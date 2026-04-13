#!/usr/bin/env python3
"""
Leichtgewichtiges IBKR/Gateway-Diagnoseskript.

Ziele:
1. Tatsächlich verwendete lokale Source-IP für die Verbindung anzeigen.
2. Prüfen, ob der konfigurierte Port nur per TCP offen ist oder ob die IBKR-API
   auch wirklich auf den Handshake antwortet.
3. Konkrete Hinweise zu wahrscheinlichen Gateway-/TWS-Fehlkonfigurationen geben.

Benutzung:
    python ibkr_diagnose.py
    python ibkr_diagnose.py --host 192.168.188.93 --port 4002
    python ibkr_diagnose.py --host 127.0.0.1 --port 4002 --client-id 991
"""

import argparse
import asyncio
import json
import os
import socket
from typing import Any, Dict, List, Optional


def test_tcp_endpoint(host: str, port: int, timeout: float = 5.0) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "host": host,
        "port": port,
        "tcp_open": False,
        "local_ip": "",
        "local_port": None,
        "remote_ip": "",
        "remote_port": None,
        "error_type": "",
        "error_message": "",
    }
    sock: Optional[socket.socket] = None
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        result["tcp_open"] = True
        local_ip, local_port = sock.getsockname()
        remote_ip, remote_port = sock.getpeername()
        result["local_ip"] = str(local_ip)
        result["local_port"] = int(local_port)
        result["remote_ip"] = str(remote_ip)
        result["remote_port"] = int(remote_port)
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error_message"] = str(exc)
    finally:
        if sock is not None:
            try:
                sock.close()
            except Exception:
                pass
    return result


def test_ibkr_api_handshake(host: str, port: int, client_id: int,
                            timeout: float = 10.0) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "attempted": False,
        "api_ready": False,
        "client_id": client_id,
        "server_version": None,
        "managed_accounts": [],
        "error_type": "",
        "error_message": "",
    }
    try:
        if os.name == "nt":
            try:
                asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
            except Exception:
                pass

        from ib_insync import IB
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error_message"] = str(exc)
        return result

    ib = IB()
    result["attempted"] = True
    try:
        ib.connect(host, port, clientId=client_id, timeout=timeout, readonly=True)
        result["api_ready"] = ib.isConnected()
        if result["api_ready"]:
            try:
                result["server_version"] = ib.client.serverVersion()
            except Exception:
                pass
            try:
                result["managed_accounts"] = list(ib.managedAccounts())
            except Exception:
                pass
    except Exception as exc:
        result["error_type"] = type(exc).__name__
        result["error_message"] = repr(exc)
    finally:
        try:
            ib.disconnect()
        except Exception:
            pass
    return result


def build_hints(selected_port: int, tcp_result: Dict[str, Any],
                api_result: Dict[str, Any]) -> List[str]:
    hints: List[str] = []
    source_ip = tcp_result.get("local_ip") or "<unbekannt>"

    if not tcp_result.get("tcp_open"):
        hints.append(
            f"Port {selected_port} ist nicht per TCP erreichbar. Prüfe Host, Port und Firewall auf dem Gateway-Rechner."
        )
        return hints

    if api_result.get("api_ready"):
        hints.append("IBKR-API-Handshake erfolgreich. Gateway/TWS antwortet korrekt auf API-Verbindungen.")
        return hints

    hints.append("TCP-Port ist offen, aber der IBKR-API-Handshake kommt nicht zustande.")
    hints.append("Prüfe in IB Gateway/TWS: 'Enable ActiveX and Socket Clients' muss aktiviert sein.")
    hints.append(
        f"Prüfe in IB Gateway/TWS: 'Allow connections from localhost only' muss aus sein, wenn du von {source_ip} zugreifst."
    )
    hints.append(f"Prüfe Trusted IPs bzw. API-Zugriff für die Client-IP {source_ip}.")
    hints.append("Prüfe, ob Gateway/TWS vollständig eingeloggt ist und keine Dialoge oder 2FA-Prompts offen sind.")
    hints.append(f"Prüfe, ob wirklich der IBKR-API-Dienst auf Port {selected_port} lauscht und nicht ein anderer Prozess.")
    if api_result.get("error_type") == "ModuleNotFoundError":
        hints.append("Kein ib_insync im aktuellen Python-Environment. Der API-Handshake-Test wurde daher lokal übersprungen.")
    return hints


def main() -> None:
    parser = argparse.ArgumentParser(description="IBKR/Gateway-Diagnose")
    parser.add_argument("--host", default=os.getenv("IBKR_HOST", "192.168.188.93"))
    parser.add_argument("--port", type=int, default=int(os.getenv("IBKR_PORT", "4002")))
    parser.add_argument("--client-id", type=int, default=int(os.getenv("IBKR_CLIENT_ID", "991")))
    parser.add_argument("--timeout", type=float, default=5.0)
    args = parser.parse_args()

    candidate_ports: List[int] = []
    for port in [args.port, 4002, 4001, 7497, 7496]:
        if port not in candidate_ports:
            candidate_ports.append(port)

    tcp_tests = [test_tcp_endpoint(args.host, port, timeout=args.timeout) for port in candidate_ports]
    selected_tcp = next((item for item in tcp_tests if item["port"] == args.port), None)
    api_test = test_ibkr_api_handshake(args.host, args.port, args.client_id, timeout=max(args.timeout, 10.0))

    result = {
        "target_host": args.host,
        "target_port": args.port,
        "probe_client_id": args.client_id,
        "tcp_tests": tcp_tests,
        "api_test": api_test,
        "hints": build_hints(args.port, selected_tcp or {}, api_test),
    }
    print(json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()