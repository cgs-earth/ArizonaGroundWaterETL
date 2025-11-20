# This is a multi-stage Dockerfile that pulls a database backup using ORAS
# and sets up a PostGIS database with the restored data.
FROM ghcr.io/oras-project/oras:v1.3.0 AS oras

WORKDIR /workspace
RUN oras pull ghcr.io/cgs-earth/arizona-groundwater-dump:latest

FROM postgis/postgis:17-3.6-alpine

# Copy the backup from the ORAS stage
COPY --from=oras /workspace/edr_backup.dump /tmp/edr_backup.dump

# Minimal restore script using socket connection
RUN echo -e 'pg_restore --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" /tmp/edr_backup.dump && rm /tmp/edr_backup.dump' \
    > /docker-entrypoint-initdb.d/restore.sh && \
    chmod +x /docker-entrypoint-initdb.d/restore.sh

# Temporary password for build only
ENV POSTGRES_PASSWORD=changeMe
ENV POSTGRES_USER=postgres
ENV POSTGRES_DB=edr
