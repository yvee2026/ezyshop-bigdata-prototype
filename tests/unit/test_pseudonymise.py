import hashlib

import pytest

from src.common.pseudonymise import pseudonymise


def test_deterministic():
    assert pseudonymise("C00042", "s1") == pseudonymise("C00042", "s1")


def test_salt_matters():
    assert pseudonymise("C00042", "s1") != pseudonymise("C00042", "s2")


def test_matches_salted_sha256_and_hides_raw_id():
    pid = pseudonymise("C00042", "s1")
    assert pid == hashlib.sha256(b"s1C00042").hexdigest()
    assert len(pid) == 64 and "C00042" not in pid


def test_unsalted_hash_differs():
    # an attacker hashing the raw ID space without the salt does not get the pseudonym
    assert pseudonymise("C00042", "s1") != hashlib.sha256(b"C00042").hexdigest()


@pytest.mark.parametrize("cid,salt", [("", "s"), ("C1", "")])
def test_rejects_empty(cid, salt):
    with pytest.raises(ValueError):
        pseudonymise(cid, salt)
