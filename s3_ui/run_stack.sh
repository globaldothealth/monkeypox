#!/bin/bash
docker compose -f docker-compose.yml up --build --force-recreate --renew-anon-volumes
