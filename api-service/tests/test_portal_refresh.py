import asyncio
import json
import os

import pytest
from fastapi import HTTPException

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost:5432/sandboxes")
os.environ.setdefault("DATABASE_TYPE", "postgres")
os.environ.setdefault("ENVIRONMENT", "development")
os.environ.setdefault("API_KEY", "client-key")
os.environ.setdefault("INTERNAL_API_KEY", "internal-key")
os.environ.setdefault("ADMIN_API_KEY", "admin-key")

from handlers import portal  # noqa: E402


class FakePortalDB:
    def __init__(self, build):
        self.build = build

    def get_template_build(self, build_id):
        if self.build and self.build.get("build_id") == build_id:
            return self.build
        return None


class FakeApiKeyDB:
    def __init__(self):
        self.created = []

    def list_api_keys_for_client(self, client_id, *, include_revoked=False):
        assert include_revoked is True
        return [{"name": "Prod", "key_id": "key-existing", "revoked_at": None}]

    def create_api_key(self, **kwargs):
        self.created.append(kwargs)
        return kwargs


def _base_context(**overrides):
    context = {
        "request": None,
        "csrf_token": "csrf",
        "registration_enabled": True,
        "admin_login_enabled": True,
        "password_min_length": 8,
        "client": {"email": "user@example.com", "display_name": "User"},
        "nav_items": [],
        "active_section": "templates",
        "template_tab": "builds",
        "sandboxes": [],
        "templates_rows": [],
        "build_rows": [],
        "api_keys": [],
        "new_api_key": None,
        "api_key_error": None,
        "running_count": 0,
        "template_count": 0,
        "build_count": 0,
    }
    context.update(overrides)
    return context


def test_template_build_json_rejects_unauthenticated(monkeypatch):
    monkeypatch.setattr(portal, "_require_client", lambda request: None)
    response = asyncio.run(portal.template_build_json(object(), "tb-1"))
    assert response.status_code == 401


def test_template_build_json_rejects_other_owner(monkeypatch):
    monkeypatch.setattr(portal, "_require_client", lambda request: {"client_id": "client-a", "is_active": True})
    monkeypatch.setattr(
        portal,
        "_db",
        lambda: FakePortalDB({"build_id": "tb-1", "owner_client_id": "client-b", "status": "running"}),
    )
    response = asyncio.run(portal.template_build_json(object(), "tb-1"))
    assert response.status_code == 404


def test_template_build_json_returns_progress(monkeypatch):
    monkeypatch.setattr(portal, "_require_client", lambda request: {"client_id": "client-a", "is_active": True})
    monkeypatch.setattr(
        portal,
        "_db",
        lambda: FakePortalDB(
            {
                "build_id": "tb-1",
                "owner_client_id": "client-a",
                "template_id": "tpl-1",
                "template_alias": "python",
                "status": "running",
                "build_log": "Step 1/2 : FROM python\n",
            }
        ),
    )
    response = asyncio.run(portal.template_build_json(object(), "tb-1"))
    payload = json.loads(response.body)
    assert response.status_code == 200
    assert payload["progress"]["percent"] == 43
    assert payload["log_lines"][0]["text"] == "Step 1/2 : FROM python"


def test_build_actions_and_clipped_labels_render():
    long_id = "tb-abcdef1234567890abcdef1234567890"
    template = portal._TEMPLATES.env.get_template("portal_shell.html")
    html = template.render(
        _base_context(
            build_rows=[
                {
                    "build_id": long_id,
                    "template_id": "template-with-a-very-long-name",
                    "requested_mode": "docker_cli",
                    "effective_mode": "docker_cli",
                    "status": "running",
                    "image_tag": "image-ref-with-a-very-long-name:latest",
                    "registry_image_ref": "",
                    "created_at": "",
                    "created_at_display": "-",
                    "completed_at": "",
                    "completed_at_display": "-",
                    "build_log": "",
                    "error_text": "",
                }
            ]
        )
    )
    assert "See progress" in html
    assert "See build logs" in html
    assert "build-detail-row" not in html
    assert "tb-abcdef1234" in html
    assert "..." in html


def test_api_key_modal_renders_after_create():
    template = portal._TEMPLATES.env.get_template("portal_shell.html")
    html = template.render(_base_context(active_section="api_keys", template_tab="list", new_api_key="sbx_secret"))
    assert 'data-modal="api-key"' in html
    assert "sbx_secret" in html
    assert "This value is shown once." in html


def test_duplicate_api_key_name_is_rejected(monkeypatch):
    db = FakeApiKeyDB()
    captured = {}

    monkeypatch.setattr(portal, "_require_csrf", lambda request, token: None)
    monkeypatch.setattr(portal, "_require_client", lambda request: {"client_id": "client-a", "is_active": True})
    monkeypatch.setattr(portal, "_db", lambda: db)
    monkeypatch.setattr(
        portal,
        "_portal_context",
        lambda request, client, **kwargs: {"api_key_error": kwargs.get("api_key_error")},
    )

    def fake_template_response(request, name, context, *, status_code=200):
        captured["name"] = name
        captured["context"] = context
        captured["status_code"] = status_code
        return captured

    monkeypatch.setattr(portal, "_template_response", fake_template_response)

    response = asyncio.run(portal.create_api_key(object(), name=" prod ", csrf_token="csrf"))

    assert response["status_code"] == 400
    assert response["context"]["api_key_error"] == 'An API key named "prod" already exists.'
    assert db.created == []


@pytest.mark.parametrize(
    "handler",
    [
        portal.admin_observability_gateways_page,
        portal.admin_observability_health_page,
        portal.admin_observability_events_page,
    ],
)
def test_observability_routes_require_admin(monkeypatch, handler):
    monkeypatch.setattr(portal, "_require_client", lambda request: {"client_id": "client-a", "is_active": True})
    monkeypatch.setattr(portal, "_request_is_admin", lambda request, client: False)
    with pytest.raises(HTTPException) as exc:
        asyncio.run(handler(object()))
    assert exc.value.status_code == 403
