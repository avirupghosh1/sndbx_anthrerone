import sys
from pathlib import Path


API_SERVICE_DIR = Path(__file__).resolve().parents[1]
if str(API_SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(API_SERVICE_DIR))

from docs_content import PAGE_BY_KEY, SECTIONS, sidebar_groups  # noqa: E402


def test_api_reference_starts_with_access_token_endpoint():
    section = next(item for item in SECTIONS if item["id"] == "api-reference")

    assert section["default"] == "auth/create-access-token"
    assert ("api-reference", "auth/create-access-token") in PAGE_BY_KEY

    groups = sidebar_groups("api-reference", section["default"])

    assert groups[0]["label"] == "Auth"
    assert groups[0]["pages"][0]["slug"] == "auth/create-access-token"
    assert groups[0]["pages"][0]["title"] == "Create access token"


def test_api_reference_groups_are_collapsible():
    groups = sidebar_groups("api-reference", "commands/run-command")
    by_label = {group["label"]: group for group in groups}

    assert all(group["collapsible"] is True for group in groups)
    assert by_label["Commands"]["open"] is True


def test_documentation_and_sdk_groups_are_collapsible():
    documentation_groups = sidebar_groups("documentation", "quickstart")
    sdk_groups = sidebar_groups("sdk-reference", "commands")

    assert documentation_groups
    assert sdk_groups
    assert all(group["collapsible"] is True for group in documentation_groups)
    assert all(group["collapsible"] is True for group in sdk_groups)
    assert any(group["open"] for group in documentation_groups)
    assert any(group["open"] for group in sdk_groups)


def test_api_reference_omits_legacy_summary_pages():
    legacy_slugs = {
        "overview",
        "auth",
        "sandboxes",
        "commands",
        "filesystem",
        "templates",
        "toolbox",
        "agents",
        "connections",
        "observability",
        "e2b-compat",
        "daytona-compat",
    }

    assert [
        slug
        for slug in sorted(legacy_slugs)
        if ("api-reference", slug) in PAGE_BY_KEY
    ] == []
