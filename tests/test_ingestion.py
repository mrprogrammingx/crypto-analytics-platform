import importlib
import os
import sys

# ensure project root is on sys.path so tests can import local modules
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)


def test_ingestion_imports():
    mod = importlib.import_module("ingestion")
    assert hasattr(mod, "run_binance_socket")
    assert callable(mod.run_binance_socket)
