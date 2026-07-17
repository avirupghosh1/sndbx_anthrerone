from template_build_progress import derive_template_build_progress, parse_build_log_lines


def test_classic_docker_progress_maps_to_build_range():
    row = {
        "status": "running",
        "build_log": "Step 1/4 : FROM python\nStep 2/4 : RUN pip install pytest\n",
    }
    progress = derive_template_build_progress(row)
    assert progress["percent"] == 43
    assert progress["phase"] == "Docker build step 2/4"
    assert progress["latest_comment"] == "Step 2/4 : RUN pip install pytest"


def test_buildkit_progress_maps_to_build_range():
    row = {
        "status": "running",
        "build_log": "#1 [internal] load build definition\n#4 [stage-0 3/6] RUN apt-get update\n",
    }
    progress = derive_template_build_progress(row)
    assert progress["percent"] == 43
    assert progress["phase"] == "Docker build step 3/6"


def test_registry_push_progress_moves_to_publish_range():
    row = {"status": "running", "build_log": "Step 4/4 : CMD bash\npushing registry image\n"}
    progress = derive_template_build_progress(row)
    assert progress["percent"] >= 82
    assert progress["phase"] == "Publishing image"


def test_success_is_complete_and_failure_keeps_latest_progress():
    success = derive_template_build_progress({"status": "success", "build_log": "Step 1/2 : FROM alpine\n"})
    failed = derive_template_build_progress(
        {
            "status": "failed",
            "build_log": "Step 1/4 : FROM alpine\n",
            "error_text": "docker build failed",
        }
    )
    assert success["percent"] == 100
    assert success["phase"] == "Template ready"
    assert failed["percent"] == 24
    assert failed["phase"] == "Build failed"
    assert failed["latest_comment"] == "docker build failed"


def test_log_lines_include_severity():
    lines = parse_build_log_lines("ok\nWARNING: deprecated\nERROR: failed\n")
    assert [line["severity"] for line in lines] == ["info", "warning", "error"]
