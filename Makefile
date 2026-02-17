up:
	docker compose up --build

down:
	docker compose down -v

logs:
	docker compose logs -f

dagster-cli:
	docker compose exec dagster_webserver dagster $(CMD)

psql:
	docker compose exec metadata_postgres psql -U "$${METADATA_PG_USER}" -d "$${METADATA_PG_DB}"

minio-shell:
	docker compose run --rm minio_init /bin/sh

dev-up:
	./dev_up.sh

test-e2e-dev:
	bash -lc 'source ./dev_bootstrap.sh && cd services/dagster && pytest -q tests/e2e'

test-e2e-docker:
	docker compose up -d --build
	docker compose exec -T dagster_webserver pytest -q tests/e2e
	docker compose down -v
