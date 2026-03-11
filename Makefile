up:
	docker compose up --build

up-detached:
	docker compose up -d --build

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

serving-connectivity-smoke:
	./infra/tests/serving_connectivity_smoke.sh

serving-perf-baseline:
	./infra/tests/serving_performance_baseline.sh

redash-bootstrap:
	docker compose up redash_bootstrap
