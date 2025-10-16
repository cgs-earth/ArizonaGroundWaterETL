dev:
	UV_ENV_FILE=.env uv run pygeoapi openapi generate pygeoapi.config.yml --output-file pygeoapi.openapi.yml
	UV_ENV_FILE=.env PYGEOAPI_CONFIG=pygeoapi.config.yml PYGEOAPI_OPENAPI=pygeoapi.openapi.yml uv run pygeoapi serve

# install dependencies
# this project uses uv to manage dependencies
deps:
	uv sync --all-groups --locked --all-packages


gen:
	uv run src/main.py

dump:
	PGPASSWORD="changeMe" pg_dump -h localhost -U postgres -d edr -F c -b -v -f edr_backup.dump