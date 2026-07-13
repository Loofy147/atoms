"""
tools.py -- simulated external side-effecting calls.

These stand in for real integrations (an OCR vendor API, a ledger/ERP
write). Each bumps a call counter on *actual* execution so the demo can
prove -- from real captured numbers, not narration -- that the gateway
only let the side effect happen once.
"""
import time
import random
import gateway


def ocr_tool(receipt_path: str) -> dict:
    gateway.bump_call_counter("ocr_tool")
    # Simulate real OCR vendor latency.
    time.sleep(2.0)
    random.seed(receipt_path)  # deterministic "detected" values per receipt
    return {
        "vendor": "Delta Air Lines",
        "amount_detected": round(random.uniform(1100, 1300), 2),
        "receipt_path": receipt_path,
    }


def ledger_write_tool(expense_id: str, amount_usd: float, decision: str) -> dict:
    gateway.bump_call_counter("ledger_write_tool")
    time.sleep(0.3)
    return {
        "ledger_entry_id": f"LEDGER-{expense_id}",
        "amount_usd": amount_usd,
        "decision": decision,
        "committed_ts": time.time(),
    }
