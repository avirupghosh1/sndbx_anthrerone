#!/usr/bin/env python3

import os
import time

from e2b import Template, wait_for_port


API_URL = os.environ.get("API_URL") or os.environ.get("E2B_API_URL")
API_KEY = os.environ.get("API_KEY") or os.environ.get("E2B_API_KEY")

if not API_URL:
    raise RuntimeError("Set API_URL or E2B_API_URL")
if not API_KEY:
    raise RuntimeError("Set API_KEY or E2B_API_KEY")

os.environ["E2B_API_URL"] = API_URL
os.environ["E2B_API_KEY"] = API_KEY
os.environ["E2B_VALIDATE_API_KEY"] = "false"

TEMPLATE_NAME = os.environ.get("TEMPLATE_NAME", "e2b-python-1783")
PYTHON_VERSION = os.environ.get("PYTHON_VERSION", "3.11")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "300"))

E2B_OPTS = {
    "api_url": API_URL,
    "api_key": API_KEY,
    "validate_api_key": False,
    "request_timeout": REQUEST_TIMEOUT,
}


def main():
    print(f"build template: {TEMPLATE_NAME}")
    print(f"api url: {API_URL}")
    template = Template().from_python_image(PYTHON_VERSION)
    template = template.set_start_cmd("python3 -m http.server 8099", wait_for_port(8099))
    build_info = Template.build(
        template,
        alias=TEMPLATE_NAME,
        skip_cache=True,
        on_build_logs=print,
        **E2B_OPTS,
    )
    print(f"Template.build: template_id={build_info.template_id} build_id={build_info.build_id}")
    print(f"Template.exists: {Template.exists(TEMPLATE_NAME, **E2B_OPTS)}")
    print(f"completed at: {int(time.time())}")


if __name__ == "__main__":
    main()
