"""Pseudonymisation of customer identifiers (GDPR Art. 4(5), Art. 32(1)(a)).

customer_pid = hex(SHA-256(salt || customer_id)).

The salt is a secret held outside the data platform (env var PSEUDO_SALT). Without it the
hash cannot be reversed by brute-forcing the small customer-ID space, and analysts only ever
see the pseudonym. The same function is used by the erasure job to locate a customer's rows.
"""
from __future__ import annotations

import hashlib
import os


def pseudonymise(customer_id: str, salt: str | None = None) -> str:
    if salt is None:
        salt = os.getenv("PSEUDO_SALT", "")
    if not salt:
        raise ValueError("PSEUDO_SALT must be set to a non-empty value")
    if not customer_id:
        raise ValueError("customer_id must be non-empty")
    return hashlib.sha256((salt + customer_id).encode("utf-8")).hexdigest()
