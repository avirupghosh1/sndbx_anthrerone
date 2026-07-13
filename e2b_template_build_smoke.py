#!/usr/bin/env python3

import os
import time

from e2b import Sandbox, Template, wait_for_port


API_URL = os.environ.get("API_URL") or os.environ.get("E2B_API_URL")
API_KEY = os.environ.get("API_KEY") or os.environ.get("E2B_API_KEY")

if not API_URL:
    raise RuntimeError("Set API_URL or E2B_API_URL")
if not API_KEY:
    raise RuntimeError("Set API_KEY or E2B_API_KEY")

os.environ["E2B_API_URL"] = API_URL
os.environ["E2B_API_KEY"] = API_KEY
os.environ["E2B_VALIDATE_API_KEY"] = "false"

RUN_ID = os.environ.get("SMOKE_RUN_ID") or str(int(time.time()))
TEMPLATE_NAME = os.environ.get("TEMPLATE_NAME", f"e2b-python-template-smoke-{RUN_ID}")
BACKGROUND_TEMPLATE_NAME = os.environ.get("BACKGROUND_TEMPLATE_NAME", f"{TEMPLATE_NAME}-bg")
TAG_NAME = os.environ.get("TAG_NAME", "smoke-tag")
PYTHON_VERSION = os.environ.get("PYTHON_VERSION", "3.11")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "1000"))
SANDBOX_TIMEOUT = int(os.environ.get("SANDBOX_TIMEOUT", "900"))

E2B_OPTS = {
    "api_url": API_URL,
    "api_key": API_KEY,
    "validate_api_key": False,
    "request_timeout": REQUEST_TIMEOUT,
}


def main():
    print(f"api url: {API_URL}")
    print("\n=== Template source constructors ===")
    Template().from_image(f"python:{PYTHON_VERSION}")
    Template().from_base_image()
    Template().from_debian_image("bookworm")
    Template().from_ubuntu_image("24.04")
    Template().from_python_image(PYTHON_VERSION)
    Template().from_node_image("20")
    Template().from_bun_image("latest")
    Template().from_template("base-template")
    if hasattr(Template(), "from_dockerfile"):
        Template().from_dockerfile(f"FROM python:{PYTHON_VERSION}\nRUN echo dockerfile-source\n")
    if hasattr(Template(), "from_aws_ecr"):
        Template().from_aws_ecr(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com/smoke:latest",
            region="us-east-1",
            access_key_id="test",
            secret_access_key="test",
        )
    if hasattr(Template(), "from_aws_registry"):
        Template().from_aws_registry(
            "123456789012.dkr.ecr.us-east-1.amazonaws.com/smoke:latest",
            access_key_id="test",
            secret_access_key="test",
            region="us-east-1",
        )
    if hasattr(Template(), "from_gcp_artifact_registry"):
        Template().from_gcp_artifact_registry(
            "gcr.io/smoke-project/smoke:latest",
            project_id="smoke-project",
            location="us",
            service_account_json={"type": "service_account", "project_id": "smoke-project"},
        )
    if hasattr(Template(), "from_gcp_registry"):
        Template().from_gcp_registry(
            "gcr.io/smoke-project/smoke:latest",
            {"type": "service_account", "project_id": "smoke-project"},
        )
    if hasattr(Template(), "from_mcp_gateway"):
        Template().from_mcp_gateway()
    if hasattr(Template(), "from_devcontainer"):
        Template().from_devcontainer()
    print("source constructors: ok")

    print("\n=== Template builder construction methods ===")
    constructed = (
        Template()
        .from_python_image(PYTHON_VERSION)
        .set_envs({"E2B_TEMPLATE_CONSTRUCT": "1"})
        .set_workdir("/tmp/e2b-template-construct")
        .set_user("root")
        .make_dir("/tmp/e2b-template-construct")
        .run_cmd("echo constructed > /tmp/e2b-template-construct/marker.txt")
        .rename(
            "/tmp/e2b-template-construct/marker.txt",
            "/tmp/e2b-template-construct/renamed.txt",
            force=True,
            user="root",
        )
        .make_symlink(
            "/tmp/e2b-template-construct/renamed.txt",
            "/tmp/e2b-template-construct/link.txt",
            user="root",
            force=True,
        )
        .remove("/tmp/e2b-template-construct/link.txt", force=True, user="root")
        .skip_cache()
        .set_ready_cmd("test -f /tmp/e2b-template-construct/renamed.txt")
    )
    if hasattr(Template, "to_json"):
        Template.to_json(constructed)
    if hasattr(Template, "to_dockerfile"):
        Template.to_dockerfile(constructed)
    print("builder construction/to_json/to_dockerfile: ok")

    print("\n=== Unsupported template build steps ===")
    if hasattr(Template(file_context_path=os.path.dirname(__file__) or ".").from_python_image(PYTHON_VERSION), "copy"):
        try:
            copy_template = (
                Template(file_context_path=os.path.dirname(__file__) or ".")
                .from_python_image(PYTHON_VERSION)
                .copy("e2b_template_build_smoke.py", "/tmp/e2b-template-copy.py")
                .set_ready_cmd("true")
            )
            Template.build(copy_template, alias=f"{TEMPLATE_NAME}-copy-unsupported", skip_cache=True, **E2B_OPTS)
        except Exception as exc:
            if "copy" not in str(exc).lower() and "not implemented" not in str(exc).lower() and "unsupported" not in str(exc).lower():
                raise
            print("copy build: not implemented as expected")
        else:
            raise AssertionError("copy build unexpectedly succeeded")
    if hasattr(Template().from_python_image(PYTHON_VERSION), "pip_install"):
        try:
            pip_template = (
                Template()
                .from_python_image(PYTHON_VERSION)
                .pip_install(["requests"])
                .set_ready_cmd("true")
            )
            Template.build(pip_template, alias=f"{TEMPLATE_NAME}-pip-unsupported", skip_cache=True, **E2B_OPTS)
        except Exception as exc:
            if "pip" not in str(exc).lower() and "unsupported" not in str(exc).lower() and "not implemented" not in str(exc).lower():
                raise
            print("pip_install build: not implemented as expected")
        else:
            raise AssertionError("pip_install build unexpectedly succeeded")
    unsupported_builders = [
        ("npm_install", lambda b: b.npm_install(["left-pad"])),
        ("bun_install", lambda b: b.bun_install(["typescript"])),
        ("apt_install", lambda b: b.apt_install(["curl"])),
        ("git_clone", lambda b: b.git_clone("https://example.com/smoke.git", "/tmp/e2b-smoke-git", branch="main", depth=1)),
        ("add_mcp_server", lambda b: b.add_mcp_server("filesystem")),
    ]
    for method_name, apply_method in unsupported_builders:
        base_builder = Template().from_python_image(PYTHON_VERSION)
        if not hasattr(base_builder, method_name):
            print(f"{method_name}: not present in this SDK version")
            continue
        try:
            unsupported_template = apply_method(base_builder).set_ready_cmd("true")
            Template.build(
                unsupported_template,
                alias=f"{TEMPLATE_NAME}-{method_name.replace('_', '-')}-unsupported",
                skip_cache=True,
                **E2B_OPTS,
            )
        except Exception as exc:
            msg = str(exc).lower()
            if method_name.split("_", 1)[0] not in msg and "unsupported" not in msg and "not implemented" not in msg:
                raise
            print(f"{method_name} build: not implemented as expected")
        else:
            raise AssertionError(f"{method_name} build unexpectedly succeeded")

    print("\n=== Template build/status/tag methods ===")
    print(f"Template.exists before build: {Template.exists(TEMPLATE_NAME, **E2B_OPTS)}")
    if hasattr(Template, "alias_exists"):
        print(f"Template.alias_exists before build: {Template.alias_exists(TEMPLATE_NAME, **E2B_OPTS)}")

    print(f"build template: {TEMPLATE_NAME}")
    template = (
        Template()
        .from_python_image(PYTHON_VERSION)
        .set_envs({"E2B_TEMPLATE_SMOKE": "ok"})
        .set_workdir("/tmp/e2b-template-smoke")
        .set_user("root")
        .make_dir("/tmp/e2b-template-smoke")
        .run_cmd("echo template-built > /tmp/e2b-template-smoke/template_marker.txt")
        .set_start_cmd("python3 -m http.server 8099", wait_for_port(8099))
    )
    build_info = Template.build(
        template,
        alias=TEMPLATE_NAME,
        cpu_count=1,
        memory_mb=1024,
        skip_cache=True,
        on_build_logs=print,
        **E2B_OPTS,
    )
    print(f"Template.build: template_id={build_info.template_id} build_id={build_info.build_id}")
    if not Template.exists(TEMPLATE_NAME, **E2B_OPTS):
        raise AssertionError("Template.exists failed after build")
    print("Template.exists after build: ok")
    if hasattr(Template, "alias_exists"):
        if not Template.alias_exists(TEMPLATE_NAME, **E2B_OPTS):
            raise AssertionError("Template.alias_exists failed after build")
        print("Template.alias_exists after build: ok")

    background_template = (
        Template()
        .from_python_image(PYTHON_VERSION)
        .run_cmd("echo background-template-built > /tmp/e2b-bg-marker")
        .set_ready_cmd("test -f /tmp/e2b-bg-marker")
    )
    background_build = Template.build_in_background(
        background_template,
        alias=BACKGROUND_TEMPLATE_NAME,
        cpu_count=1,
        memory_mb=512,
        skip_cache=True,
        **E2B_OPTS,
    )
    print(f"Template.build_in_background: template_id={background_build.template_id} build_id={background_build.build_id}")
    last_status = None
    for _ in range(120):
        last_status = Template.get_build_status(background_build, logs_offset=0, **E2B_OPTS)
        print(f"Template.get_build_status: {last_status.status}")
        if str(last_status.status).lower() in {"ready", "error"}:
            break
        time.sleep(2)
    if last_status is None or str(last_status.status).lower() != "ready":
        raise AssertionError(f"background build did not become ready: {getattr(last_status, 'status', None)}")

    assigned = Template.assign_tags(TEMPLATE_NAME, [TAG_NAME], **E2B_OPTS)
    print(f"Template.assign_tags: {getattr(assigned, 'tags', assigned)}")
    tags = Template.get_tags(TEMPLATE_NAME, **E2B_OPTS)
    tag_values = [getattr(tag, "tag", str(tag)) for tag in tags]
    print(f"Template.get_tags: {tag_values}")
    if TAG_NAME not in tag_values:
        raise AssertionError("Template.get_tags did not include assigned tag")
    if not Template.exists(f"{TEMPLATE_NAME}:{TAG_NAME}", **E2B_OPTS):
        raise AssertionError("Template.exists failed for tagged template")
    Template.remove_tags(TEMPLATE_NAME, [TAG_NAME], **E2B_OPTS)
    removed_tags = [getattr(tag, "tag", str(tag)) for tag in Template.get_tags(TEMPLATE_NAME, **E2B_OPTS)]
    if TAG_NAME in removed_tags:
        raise AssertionError("Template.remove_tags did not remove tag")
    print("Template.remove_tags: ok")

    print("\n=== Template create sanity ===")
    sandbox = Sandbox.create(
        TEMPLATE_NAME,
        timeout=SANDBOX_TIMEOUT,
        metadata={"suite": "e2b-template-smoke", "run_id": RUN_ID},
        envs={"SMOKE_ENV": "template"},
        secure=True,
        allow_internet_access=True,
        **E2B_OPTS,
    )
    try:
        result = sandbox.commands.run("test -f /tmp/e2b-template-smoke/template_marker.txt && echo ok", timeout=60)
        if result.exit_code != 0 or "ok" not in result.stdout:
            raise AssertionError(f"template marker check failed: exit={result.exit_code} stdout={result.stdout!r} stderr={result.stderr!r}")
        print(f"Sandbox.create from built template: {sandbox.sandbox_id}")
    finally:
        sandbox.kill()
        print("sandbox.kill: ok")

    print(f"completed at: {int(time.time())}")


if __name__ == "__main__":
    main()
