# Running python files in constrained memory
#     docker buildx create --use --driver=docker-container --name container --bootstrap --build-ops=memory=2G
#     docker buildx build . --builder=container --progress plain -o dist

FROM python:3.12 AS base
RUN pip install dask[complete] pyarrow rich click more-itertools
WORKDIR /app
COPY *.py .
RUN \
    --mount=type=bind,target=/app/data,source=data \
    python counter.py "data/seq*.parquet" 

FROM scratch
WORKDIR /
# dummy copy to force base build
COPY --from=base /app/*.py .
