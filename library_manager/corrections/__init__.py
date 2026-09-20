"""Opt-in corrections feed primitives; no polling starts on import.

The canonical event validator and application/review workflow are deliberately
separate from transport and durable ingestion. No unvalidated event is accepted.
"""
