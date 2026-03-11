```bash
uv sync --dev
uv run pre-commit install
```

```bash
PYTHONPATH=src uv run python -m main
```

```bash
uv run pre-commit run --all-files
uv run pre-commit run --files src/infra/time.py
```

```bash
uv run ruff check .
uv run ruff check . --fix
```

```bash
uv run ruff format --check .
uv run ruff format .
```
