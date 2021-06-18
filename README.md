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
POSTGRES_PASSWORD=password1 -d postgres
```

2. Start the docker api-search

```bash
docker run <container id>
```

