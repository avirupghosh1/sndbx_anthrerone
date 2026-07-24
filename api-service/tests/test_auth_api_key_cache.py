from types import SimpleNamespace

import pytest

from middleware import auth


class FakeAuthDB:
    def __init__(self):
        self.get_calls = 0
        self.touch_calls = 0
        self.row = {
            "client_id": "client-a",
            "key_id": "key-a",
            "name": "prod",
            "key_prefix": "e2b_dead",
            "email": "user@example.com",
            "display_name": "User",
            "is_active": True,
            "revoked_at": None,
        }

    def get_api_key_principal(self, key_hash):
        self.get_calls += 1
        return dict(self.row)

    def touch_api_key_used(self, key_id):
        self.touch_calls += 1
        return True


@pytest.fixture(autouse=True)
def clear_cache():
    auth.clear_api_key_auth_cache()
    yield
    auth.clear_api_key_auth_cache()


def test_api_key_auth_cache_skips_db_after_first_success(monkeypatch):
    db = FakeAuthDB()
    monkeypatch.setattr(
        auth,
        "get_config",
        lambda: SimpleNamespace(AUTH_API_KEYS_ENABLED=True, AUTH_API_KEY_CACHE_TTL_SEC=30.0),
    )
    monkeypatch.setattr(auth, "ensure_bootstrap_client_and_key", lambda: None)
    monkeypatch.setattr(auth, "_db", lambda: db)

    first = auth.authenticate_api_key_value("e2b_deadbeef")
    second = auth.authenticate_api_key_value("e2b_deadbeef")

    assert first == second
    assert db.get_calls == 1
    assert db.touch_calls == 1
