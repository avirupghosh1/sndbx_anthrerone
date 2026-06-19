# Why `pip install agentlib` does not fix `check_Code.py`

`check_Code.py` imports:

- `agentlib.claude`
- `agentlib.events`
- `agentlib.exceptions`
- `agentlib.session`

Those modules come from **Custodian’s internal `agentlib` tree** (the same codebase that ships `agentlib-e2b-server`, `python -m agentlib.e2b_server.s3_sync`, etc.). It is **not** the package named **`agentlib` on PyPI**.

## What PyPI `agentlib` is

[PyPI `agentlib`](https://pypi.org/project/agentlib/) is a **different project**: “agents for control and simulation of **energy systems**”. It does not define `agentlib.claude` or the E2B host helpers. Installing it will not satisfy `check_Code.py`.

## What you need for `scripts/test_dropin_agentlib_e2e.py`

1. A checkout of the **real** `agentlib` repo (the one your team uses with Custodian), with the usual layout:

   - `agentlib/src/agentlib/claude.py`
   - `agentlib/pyproject.toml` with an `[e2b]` extra (or equivalent)

2. Install it **editable** into the same venv you use for the test:

   ```bash
   pip install -e "/path/to/agentlib[e2b]"
   ```

   or, without installing the package, extend `PYTHONPATH`:

   ```bash
   export PYTHONPATH="/path/to/agentlib/src:${PYTHONPATH}"
   ```

3. Other dependencies that `agentlib` pulls in (per its `pyproject.toml`) must be present in that venv.

This repo’s `DockerFile` (capital **F**) shows the intended pattern: `COPY agentlib /app/agentlib` and `PYTHONPATH="/app/agentlib/src"` plus `pip install -e "/app/agentlib[e2b]"`. That `agentlib` directory is **not** vendored here; you obtain it from your internal source tree.

## If you do not have internal `agentlib`

You can still validate the **API + shim + Docker** path without `check_Code.py`:

```bash
python api_server/scripts/test_dropin_integration.py
bash api_server/scripts/test_dropin_smoke.sh
```

To exercise **the same WebSocket + `prompt` → `result` protocol** the Custodian / Claude path uses—without importing `agentlib`—run:

```bash
python scripts/test_dropin_generic_claude_ws_e2e.py
```

See `docs/E2B_DROPIN_TESTING.md` §5b. The full Custodian-style **`execute_turn`** test (`test_dropin_agentlib_e2e.py`) is only useful when the real `agentlib` is on `PYTHONPATH` or installed editable.
