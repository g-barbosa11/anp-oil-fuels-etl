#!/bin/bash
source scripts/project_config.sh 

build_airflow_image () {
    if ! docker image inspect $IMAGE_URI > /dev/null 2>&1; then
        docker build -t $IMAGE_URI -f docker/Dockerfile .
    else
        echo "Image $IMAGE_URI already installed."
    fi
}


start_airflow () {
    build_airflow_image
    docker-compose -f docker/docker-compose.yaml up -d
}


stop_airflow () {
    docker-compose -f docker/docker-compose.yaml down -v
}


func="$1"
case "$func" in
    build_airflow_image)
        build_airflow_image
        ;;
    "start_airflow")
        start_airflow
        ;;
    "stop_airflow")
        stop_airflow
        ;;
    *)
        echo "Func not found: $func"
        exit 1
        ;;
esac