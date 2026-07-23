import importlib.util
from pathlib import Path


API_SERVICE_DIR = Path(__file__).resolve().parents[1]

_MODULE_PATH = API_SERVICE_DIR / "handlers" / "guest_proxy_utils.py"
_SPEC = importlib.util.spec_from_file_location("guest_proxy_utils", _MODULE_PATH)
assert _SPEC and _SPEC.loader
_MODULE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MODULE)
forward_headers = _MODULE.forward_headers
gateway_http_url = _MODULE.gateway_http_url
gateway_ws_url = _MODULE.gateway_ws_url


def test_gateway_ws_url_targets_gateway_root():
    assert gateway_ws_url("http://runtime-gateway.svc:8080") == "ws://runtime-gateway.svc:8080/"
    assert gateway_ws_url("https://gateway.example/internal") == "wss://gateway.example/internal/"


def test_gateway_http_url_targets_gateway_base():
    assert gateway_http_url("http://runtime-gateway.svc:8080") == "http://runtime-gateway.svc:8080"
    assert gateway_http_url("wss://gateway.example/internal") == "https://gateway.example/internal"


def test_forward_headers_encode_gateway_route_and_tokens():
    headers = forward_headers(
        {
            "origin": "https://app.example",
            "authorization": "Bearer guest-token",
        },
        sandbox_id="sb-123",
        guest_port=8765,
        traffic_access_token="traffic-token",
    )

    assert headers["x-runtime-gateway-forwarded"] == "1"
    assert headers["x-sandbox-id"] == "sb-123"
    assert headers["x-guest-port"] == "8765"
    assert headers["origin"] == "https://app.example"
    assert headers["authorization"] == "Bearer guest-token"
    assert headers["e2b-traffic-access-token"] == "traffic-token"


def test_forward_headers_do_not_leak_api_bearer_auth_to_guest():
    headers = forward_headers(
        {
            "authorization": "Bearer api-access-token",
            "x-guest-authorization": "Bearer guest-token",
        },
        sandbox_id="sb-123",
        guest_port=8765,
        traffic_access_token=None,
        api_auth_used_authorization=True,
    )

    assert headers["authorization"] == "Bearer guest-token"
    assert "e2b-traffic-access-token" not in headers
