# Docker for api-search

## Installation

Pull Docker image of postgres:

```bash
docker pull postgres
```

Create Build of api-search:

```bash
docker build <Path of the Dockerfile>
```
Get the container id

## Usage

To use the api-search:

1. start the postgres docker image

```bash
docker run --name=demo \
-p 5432:5432  -e \ 
POSTGRES_PASSWORD=<ADD-YOUR-PASSWORD> -d postgres
```
Same password needs to be set in table_metadata.yaml

2. Start the docker api-search

```bash
docker run <container id>
```

## Table Metadata

Place to store all the config related information

