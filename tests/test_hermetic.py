"""The suite's own isolation (see `hermetic_environment` in conftest.py).

Two things used to make it depend on where it ran: a proxy variable in the developer's or
CI's shell sent the real-`requests` tests to the proxy instead of the loopback fake, and
nothing stopped a test from quietly reaching for the real network.
"""

from __future__ import annotations

import os
import socket

import pytest
import requests

from conftest import PROXY_VARIABLES, clear_proxy_env


def test_the_autouse_fixture_has_cleared_the_environment_before_every_test():
    """No explicit call here: this pins that hermetic_environment itself does the clearing."""
    assert os.environ.get("NO_PROXY") == os.environ.get("no_proxy") == "127.0.0.1,localhost"
    assert [v for v in PROXY_VARIABLES if v in os.environ] == []


@pytest.mark.parametrize("variable", PROXY_VARIABLES)
def test_proxy_variables_are_cleared(monkeypatch, variable):
    monkeypatch.setenv(variable, "http://127.0.0.1:9")      # nothing listens on port 9
    clear_proxy_env(monkeypatch)
    assert variable not in os.environ


@pytest.mark.parametrize("variable", PROXY_VARIABLES)
def test_loopback_bypasses_a_proxy_even_one_set_after_the_clearing(monkeypatch, fake_server, variable):
    """NO_PROXY is the second layer: it also covers the operating system's own proxy
    settings, which requests falls back to when the environment names none."""
    clear_proxy_env(monkeypatch)
    monkeypatch.setenv(variable, "http://127.0.0.1:9")
    assert requests.utils.get_environ_proxies(fake_server.url) == {}
    assert requests.get(fake_server.url + "/ping", timeout=5).status_code == 200


def test_only_loopback_connections_are_allowed():
    """TEST-NET-1 (192.0.2.0/24) and .invalid are reserved: nothing real is ever behind them."""
    with pytest.raises(OSError, match="only loopback"):
        socket.getaddrinfo("example.invalid", 443)
    with pytest.raises(OSError, match="only loopback"):
        socket.create_connection(("192.0.2.1", 80), timeout=1)
    with socket.socket() as s:
        s.settimeout(1)
        with pytest.raises(OSError, match="only loopback"):
            s.connect(("192.0.2.1", 80))
        with pytest.raises(OSError, match="only loopback"):
            s.connect_ex(("192.0.2.1", 80))


def test_loopback_connections_still_work(fake_server):
    host, port = fake_server.url.removeprefix("http://").split(":")
    with socket.create_connection((host, int(port)), timeout=5):
        pass
