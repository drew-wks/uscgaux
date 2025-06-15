import os
import importlib
import pytest

import env_config


def test_env_config_cli(monkeypatch):
    monkeypatch.setenv("RUN_CONTEXT", "cli")
    monkeypatch.setenv("FORCE_USER_AUTH", "false")
    cfg = importlib.reload(env_config).env_config()
    assert cfg["RUN_CONTEXT"] == "cli"
    assert cfg["FORCE_USER_AUTH"] is False


def test_rag_config_missing_key():
    with pytest.raises(KeyError):
        env_config.rag_config("missing")
