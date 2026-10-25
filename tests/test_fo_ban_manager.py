import pytest
import os
from core.fo_ban_manager import get_banned_fo_symbols, is_symbol_banned

def test_fo_ban_manager_empty(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    banned = get_banned_fo_symbols()
    assert len(banned) == 0
    assert is_symbol_banned("TCS") is False

def test_fo_ban_manager_with_file(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    os.makedirs("data", exist_ok=True)
    with open("data/fo_secban.csv", "w") as f:
        f.write("Symbol\nSAIL\nGNFC\nPNB\n")
        
    banned = get_banned_fo_symbols()
    assert "SAIL" in banned
    assert "GNFC" in banned
    assert "PNB" in banned
    assert is_symbol_banned("SAIL") is True
    assert is_symbol_banned("RELIANCE") is False
