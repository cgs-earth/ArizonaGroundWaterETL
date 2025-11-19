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
backup:
	PGPASSWORD="changeMe" pg_dump -h localhost -U postgres -d edr -F c -b -v -f edr_backup.dump

# restore the data into the db; useful for testing
# need to drop the database first otherwise restoring will cause issues due to a duplicate relation
restore:
	PGPASSWORD="changeMe" psql -h localhost -U postgres -c "DROP DATABASE IF EXISTS edr;"
	PGPASSWORD="changeMe" psql -h localhost -U postgres -c "CREATE DATABASE edr;"
	PGPASSWORD="changeMe" pg_restore -h localhost -U postgres -d edr edr_backup.dump

# push the data backup to ghcr.io
push_to_registry:
	oras push ghcr.io/cgs-earth/arizona-groundwater-dump:latest  edr_backup.dump:application/octet-stream --username internetofwater --password ${PERSONAL_ACCESS_TOKEN}

check_locations_with_joined_wells_metadata
	# assuming that the join was done properly this will show multiple location ids 
	PGPASSWORD="changeMe" psql -h localhost -U postgres -d edr -c "SELECT location_id FROM edr_quickstart.locations WHERE char_length(location_id) > 7 AND EXISTS (SELECT 1 FROM jsonb_object_keys(properties) AS key WHERE key LIKE 'WELLS_55%');"
