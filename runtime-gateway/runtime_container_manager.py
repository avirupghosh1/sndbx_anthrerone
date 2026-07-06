from __future__ import annotations

"""Local source wrapper for the shared Docker execution implementation.

The production runtime-gateway image overwrites this file with
`api-service/orchestrator/container_manager.py` in the Dockerfile, so the Docker
execution behavior stays shared between local source and built images.
"""

import importlib.util
from pathlib import Path

_source = Path(__file__).resolve().parent.parent / "api-service" / "orchestrator" / "container_manager.py"
_spec = importlib.util.spec_from_file_location("_shared_container_manager", _source)
if _spec is None or _spec.loader is None:
    raise ImportError(f"cannot load shared ContainerManager from {_source}")
_module = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(_module)

ContainerConfig = _module.ContainerConfig
ContainerManager = _module.ContainerManager
