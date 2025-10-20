dev:
	UV_ENV_FILE=.env uv run pygeoapi openapi generate pygeoapi.config.yml --output-file pygeoapi.openapi.yml
	UV_ENV_FILE=.env PYGEOAPI_CONFIG=pygeoapi.config.yml PYGEOAPI_OPENAPI=pygeoapi.openapi.yml uv run pygeoapi serve

# install dependencies
# this project uses uv to manage dependencies
deps:
	uv sync --all-groups --locked --all-packages

# move the data into the database
load_xlsx:
	uv run src/main.py --xlsx

add_metadata:
	uv run src/main.py --wells55

# generate a sql dump of the data itself
dump:
	PGPASSWORD="changeMe" pg_dump -h localhost -U postgres -d edr -F c -b -v -f edr_backup.dump