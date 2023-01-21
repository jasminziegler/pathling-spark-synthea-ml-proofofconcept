#!/bin/bash
set -e

JUPYTER_ONLY="jupyter"#"$1"

printf "Building spark images\n\n"
cd spark
source ./build.sh

printf "Building pathling image\n\n"
cd ..
docker build -f Dockerfile -t jupyter-pathling .

# start container-framework
#docker-compose -f docker-compose.dev.yml up -d
docker-compose -f docker-compose.qs.yml up -d

printf "waiting 30 seconds for mock-data-loader to finish\n\n"
sleep 30

if [[ $JUPYTER_ONLY == "jupyter" ]]
then
    docker logs -f jupyter-pathling
else
    # check logs of pathling container for getting URL
    docker exec -it --user jovyan jupyter-pathling bash -c "source /usr/local/bin/before-notebook.d/spark-config.sh && \
        python work/kafka_stream_con.py"
fi
