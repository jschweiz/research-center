BACKEND_DIR := apps/backend
BACKEND_VENV := $(BACKEND_DIR)/.venv
BACKEND_PYTHON := $(BACKEND_VENV)/bin/python
BACKEND_RUNTIME_REQUIREMENTS := /tmp/research-center-backend-runtime.txt

.PHONY: backend-install backend-install-embeddings backend-test backend-lint backend-audit backend-migrate backend-upsert-sources backend-run worker-run web-install web-run web-typecheck web-build web-audit release-check

backend-install:
	cd $(BACKEND_DIR) && python3 -m venv .venv && .venv/bin/python -m pip install -e ".[dev]"

backend-install-embeddings:
	@if [ ! -x "$(BACKEND_PYTHON)" ]; then echo "Missing $(BACKEND_PYTHON). Run 'make backend-install' first." >&2; exit 1; fi
	cd $(BACKEND_DIR) && .venv/bin/python -m pip install -e ".[embeddings]"

backend-test:
	cd $(BACKEND_DIR) && .venv/bin/python -m pytest

backend-lint:
	cd $(BACKEND_DIR) && .venv/bin/python -m ruff check app

backend-audit:
	cd $(BACKEND_DIR) && uv export --frozen --no-dev --format requirements-txt --no-editable --no-hashes --no-emit-project --output-file $(BACKEND_RUNTIME_REQUIREMENTS) > /dev/null
	uv tool run --from pip-audit pip-audit -r $(BACKEND_RUNTIME_REQUIREMENTS) --progress-spinner off --no-deps --disable-pip

backend-migrate:
	cd $(BACKEND_DIR) && .venv/bin/python -m alembic upgrade head

backend-upsert-sources:
	PYTHONPATH=$(BACKEND_DIR) $(BACKEND_PYTHON) $(BACKEND_DIR)/scripts/upsert_frontier_sources.py

backend-run:
	@if [ ! -x "$(BACKEND_PYTHON)" ]; then echo "Missing $(BACKEND_PYTHON). Run 'make backend-install' first." >&2; exit 1; fi
	$(BACKEND_PYTHON) -m uvicorn app.main:app --app-dir $(BACKEND_DIR) --reload

worker-run:
	@if [ ! -x "$(BACKEND_PYTHON)" ]; then echo "Missing $(BACKEND_PYTHON). Run 'make backend-install' first." >&2; exit 1; fi
	$(BACKEND_PYTHON) -m celery --workdir $(BACKEND_DIR) -A app.tasks.celery_app.celery_app worker --loglevel=info

web-install:
	cd apps/web && npm install

web-run:
	cd apps/web && npm run dev

web-typecheck:
	cd apps/web && npm run typecheck

web-build:
	cd apps/web && npm run build

web-audit:
	cd apps/web && npm audit --omit=dev

release-check: backend-lint backend-test backend-audit web-typecheck web-build web-audit
