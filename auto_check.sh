#!/bin/bash

cp netem/main.c setup/main.c
cp netem/run.sh setup/run.sh
cp netem/config.csv setup/config.csv

cd setup
./stop_container.sh
./start-container.sh
CONTAINER_ID=$(docker ps --format "{{.ID}}" | head -n 1)
docker exec -it -u root "$CONTAINER_ID" /bin/bash -c "cd challenge-app/netem && chmod +x run.sh && ./run.sh; exec /bin/bash"