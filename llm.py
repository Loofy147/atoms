"""
llm.py -- the "reasoning" step a Worker performs.

Honesty note: this sandbox has no ANTHROPIC_API_KEY configured, so the demo
run in this conversation uses the heuristic fallback below, not a live
model call -- and it says so in its output rather than pretending
otherwise. Point this at a real key (env var ANTHROPIC_API_KEY) in a real
deployment and it will call the actual Messages API instead.
"""
import os
import json
import urllib.request


def classify_expense(expense: dict) -> dict:
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if api_key:
        return _classify_via_anthropic(expense, api_key)
    return _classify_via_heuristic(expense)


def _classify_via_heuristic(expense: dict) -> dict:
    amount = expense["amount_usd"]
    category = expense.get("category", "unknown")
    flagged_categories = {"entertainment", "gifts", "unspecified"}
    risk = "HIGH_RISK" if (amount > 500 or category in flagged_categories) else "LOW_RISK"
    return {
        "risk": risk,
        "reasoning": (
            f"amount=${amount} category={category!r}: "
            f"{'exceeds $500 auto-approve threshold or flagged category' if risk == 'HIGH_RISK' else 'within auto-approve bounds'}"
        ),
        "engine": "heuristic-fallback (no ANTHROPIC_API_KEY set)",
    }


def _classify_via_anthropic(expense: dict, api_key: str) -> dict:
    prompt = (
        "Classify this expense as HIGH_RISK or LOW_RISK for finance approval. "
        "Respond ONLY with JSON: {\"risk\": \"HIGH_RISK\"|\"LOW_RISK\", \"reasoning\": \"...\"}.\n"
        f"Expense: {json.dumps(expense)}"
    )
    body = json.dumps({
        "model": "claude-sonnet-4-6",
        "max_tokens": 300,
        "messages": [{"role": "user", "content": prompt}],
    }).encode()
    req = urllib.request.Request(
        "https://api.anthropic.com/v1/messages",
        data=body,
        headers={
            "content-type": "application/json",
            "x-api-key": api_key,
            "anthropic-version": "2023-06-01",
        },
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        data = json.loads(resp.read())
    text = "".join(b["text"] for b in data["content"] if b["type"] == "text")
    parsed = json.loads(text)
    parsed["engine"] = "anthropic-live"
    return parsed
