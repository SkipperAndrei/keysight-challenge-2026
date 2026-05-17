#!/bin/bash

cp netem/main.c setup/main.c
cp netem/run.sh setup/run.sh

cd setup
./stop_container.sh
./start-container.sh
CONTAINER_ID=$(docker ps --format "{{.ID}}" | head -n 1)
docker exec -it "$CONTAINER_ID" /bin/bash
