BACKEND_DIR := apps/backend
BACKEND_VENV := $(BACKEND_DIR)/.venv
BACKEND_PYTHON := $(BACKEND_VENV)/bin/python
BACKEND_RUNTIME_REQUIREMENTS := /tmp/research-center-backend-runtime.txt
BACKEND_TEST_FILES := \
	app/tests/test_advanced_enrichment.py \
	app/tests/test_config.py \
	app/tests/test_schema_bootstrap.py \
	app/tests/test_vault_briefs.py \
	app/tests/test_vault_items.py \
	app/tests/test_publishing.py \
	app/tests/test_local_control.py \
	app/tests/test_ops.py \
	app/tests/test_vault_sources.py \
	app/tests/test_vault_source_routes.py

.PHONY: backend-install backend-install-embeddings backend-test backend-lint backend-audit backend-migrate backend-export-sqlite-to-vault backend-audit-vault backend-sync-vault backend-run web-install web-run web-typecheck web-build web-audit vault-submodule-init release-check

backend-install:
	cd $(BACKEND_DIR) && python3 -m venv .venv && .venv/bin/python -m pip install -e ".[dev]"

backend-install-embeddings:
	@if [ ! -x "$(BACKEND_PYTHON)" ]; then echo "Missing $(BACKEND_PYTHON). Run 'make backend-install' first." >&2; exit 1; fi
	cd $(BACKEND_DIR) && .venv/bin/python -m pip install -e ".[embeddings]"

backend-test:
	cd $(BACKEND_DIR) && .venv/bin/python -m pytest $(BACKEND_TEST_FILES)

backend-lint:
	cd $(BACKEND_DIR) && .venv/bin/python -m ruff check app

backend-audit:
	cd $(BACKEND_DIR) && uv export --frozen --no-dev --format requirements-txt --no-editable --no-hashes --no-emit-project --output-file $(BACKEND_RUNTIME_REQUIREMENTS) > /dev/null
	uv tool run --from pip-audit pip-audit -r $(BACKEND_RUNTIME_REQUIREMENTS) --progress-spinner off --no-deps --disable-pip

backend-migrate:
	@echo "No migrations are required in file-native vault mode."

backend-export-sqlite-to-vault:
	cd $(BACKEND_DIR) && .venv/bin/python -m app.tasks.jobs export-sqlite-to-vault-inline

backend-audit-vault:
	cd $(BACKEND_DIR) && .venv/bin/python -m app.tasks.jobs audit-vault-inline

backend-sync-vault:
	cd $(BACKEND_DIR) && .venv/bin/python -m app.tasks.jobs sync-vault-inline

backend-run:
	@if [ ! -x "$(BACKEND_PYTHON)" ]; then echo "Missing $(BACKEND_PYTHON). Run 'make backend-install' first." >&2; exit 1; fi
	$(BACKEND_PYTHON) -m uvicorn app.main:app --app-dir $(BACKEND_DIR) --reload --host 0.0.0.0

vault-submodule-init:
	git submodule update --init --recursive

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
