import pytest
from kdbpy import lib

def test_proof_of_concept():
    assert lib.proof_of_concept('foo')
