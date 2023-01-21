# pathling-spark-synthea-ml-POC

## Description

- examplary data generated with synthea is not included in this repository
- a SPARK setup
    - master on http://localhost:8083/ 
- a pathling container built from a [Dockerfile](Dockerfile) (where the pathling python API is installed and important pyspark submit args are defined)

## Start Container

In order to start the containers with kafka and mock-data-loader + pathling container including jupyter lab, run the following command:

```bash
# if not executable, first run "chmod +x start.sh"
./start.sh
```

## Stop Container-Framework

```bash
# if not executable, first run "chmod +x stop.sh"
./stop.sh
```

## Use JupyterLab 

In order to use the jupyter lab, just run the following command and click on the URL to open Jupyter in a browser:

```bash
docker logs -f jupyter-pathling
```
