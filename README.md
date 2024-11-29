# dask_test
Validate how dask DataFrames work with parquet files

# generator

Produce parquet file:
- fine grained: one record per row group
- one shot: one row group per file

## installing

```sh
pip install 'dask[complete]' pyarrow rich click more-itertools
```

## running

```sh
python generate_seq.py
```

# counter

Count records using dask. No OOMs expected and reasonable performance

see Dockefile to running python in constrained memory

