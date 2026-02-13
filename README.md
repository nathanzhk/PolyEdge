```bash
uv sync --dev
uv run pre-commit install
```

```bash
uv run pre-commit run --all-files
uv run pre-commit run --files common/time.py
```

```bash
uv run ruff check .
uv run ruff check . --fix
```

```bash
uv run ruff format --check .
uv run ruff format .
```
