"""BONUS — an LLM inside the pipeline (slide "LLM là một bước transform").

The support team wants an LLM pre-triage label on every live ticket
(gold_ticket_labels), to compare with the human `category` and to triage new
tickets faster. An LLM step is a transform like any other — except it is
expensive, slow and NOT deterministic, so the slide's four rules apply:

  1. key = hash(input) + model + prompt version  -> a re-run makes 0 LLM calls;
     changing the prompt re-labels everything ON PURPOSE
  2. force a structured output, validate it; invalid -> quarantine, never Gold
  3. estimate the cost BEFORE running (rows x tokens x price)
  4. LLM labels are versioned data (model + prompt_version stored on every row)

The shipped `label_tickets` is the NAIVE version: it calls the model for every
ticket on every run and writes whatever comes back. Your bonus task is to make
`python -m scripts.bonus_llm` print BONUS PASS. Zero-key: `FakeLLM` stands in for a
real model (swap in any provider via .env if you like — the pipeline is the same).
"""
from __future__ import annotations

import hashlib
import json
import re

import duckdb

MODEL = "fake-llm-2026-09"
PROMPT_VERSION = "triage-v1"
ALLOWED_LABELS = ("bug", "billing", "other")
PRICE_PER_1K_TOKENS_USD = 0.002          # pretend price, for the cost estimate


PROMPT_TEMPLATE = """You triage customer-support tickets.
Answer ONLY with JSON: {{"label": "bug" | "billing" | "other"}}.
Ticket: {text}"""


class FakeLLM:
    """Deterministic stand-in for a chat model. Counts calls and tokens."""

    def __init__(self, model: str = MODEL) -> None:
        self.model = model
        self.calls = 0
        self.tokens = 0

    def complete(self, prompt: str) -> str:
        self.calls += 1
        self.tokens += len(prompt.split()) + 8
        text = prompt.lower()
        if "xuất" in text:
            return 'Sure! Here is the label: {"label": "export"}'   # off-schema answer
        if re.search(r"crash|lỗi|sso|đăng nhập|chatbot", text):
            return '{"label": "bug"}'
        if re.search(r"tiền|hoá đơn|thanh toán|gói|vat", text):
            return '{"label": "billing"}'
        return '{"label": "other"}'


def estimate_tokens(texts: list[str]) -> int:
    return sum(len(PROMPT_TEMPLATE.format(text=t).split()) + 8 for t in texts)


def parse_label(raw: str) -> str | None:
    """Pull {"label": ...} out of the model's answer; None if it is not valid."""
    m = re.search(r"\{.*\}", raw, flags=re.S)
    if not m:
        return None
    try:
        label = json.loads(m.group(0)).get("label")
    except json.JSONDecodeError:
        return None
    return label if label in ALLOWED_LABELS else None


def live_tickets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    return con.execute("""
        SELECT ticket_id, subject || '. ' || body AS text
        FROM silver_tickets
        WHERE NOT is_deleted
        ORDER BY ticket_id
    """).fetchall()


def input_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def label_tickets(con: duckdb.DuckDBPyConnection, llm: FakeLLM) -> dict:
    """Label every live ticket, calling the LLM only for (input, model, prompt) never seen.

    Every raw answer is cached — off-schema ones too, so a re-run does not pay
    for them again. Valid labels go to Gold; invalid ones to llm_label_quarantine.
    """
    prompt_version = PROMPT_VERSION      # read at call time: a new version re-labels on purpose
    con.execute("""CREATE TABLE IF NOT EXISTS llm_label_cache (
        input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR, raw VARCHAR,
        PRIMARY KEY (input_hash, model, prompt_version))""")

    tickets = [(tid, text, input_hash(text)) for tid, text in live_tickets(con)]
    cached = {h for (h,) in con.execute(
        "SELECT input_hash FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [llm.model, prompt_version]).fetchall()}
    todo = {h: text for _, text, h in tickets if h not in cached}

    # Rule 3: estimate the cost of what we are about to send, before sending it.
    est_tokens = estimate_tokens(list(todo.values()))
    est_usd = est_tokens / 1000 * PRICE_PER_1K_TOKENS_USD

    calls_before = llm.calls
    for h, text in sorted(todo.items()):
        raw = llm.complete(PROMPT_TEMPLATE.format(text=text))
        con.execute("INSERT INTO llm_label_cache VALUES (?, ?, ?, ?)",
                    [h, llm.model, prompt_version, raw])

    answers = dict(con.execute(
        "SELECT input_hash, raw FROM llm_label_cache WHERE model = ? AND prompt_version = ?",
        [llm.model, prompt_version]).fetchall())
    good, bad = [], []
    for ticket_id, _, h in tickets:
        raw = answers[h]
        label = parse_label(raw)
        if label is None:
            bad.append((ticket_id, h, llm.model, prompt_version, raw, "off-schema answer"))
        else:
            good.append((ticket_id, label, h, llm.model, prompt_version))

    # Gold and quarantine are derived from the cache -> rebuilt in full, idempotent.
    con.execute("""CREATE OR REPLACE TABLE gold_ticket_labels (
        ticket_id VARCHAR, label VARCHAR, input_hash VARCHAR, model VARCHAR,
        prompt_version VARCHAR)""")
    con.execute("""CREATE OR REPLACE TABLE llm_label_quarantine (
        ticket_id VARCHAR, input_hash VARCHAR, model VARCHAR, prompt_version VARCHAR,
        raw VARCHAR, reason VARCHAR)""")
    if good:
        con.executemany("INSERT INTO gold_ticket_labels VALUES (?, ?, ?, ?, ?)", good)
    if bad:
        con.executemany("INSERT INTO llm_label_quarantine VALUES (?, ?, ?, ?, ?, ?)", bad)
    return {"labeled": len(good), "quarantined": len(bad),
            "calls": llm.calls - calls_before, "cache_hits": len(tickets) - len(todo),
            "est_tokens": est_tokens, "est_usd": round(est_usd, 6)}
