import os
import random
import sys
import click
import pyarrow.parquet as pq
import pyarrow as pa
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn,TransferSpeedColumn
from more_itertools import consume, ichunked


def gen_random(rnd, total):
    for _ in range(total):
        yield rnd.random()

def save_parquet(file_name, generator, chunk_size):
    dummy_lst = [{'float': 1.2}]
    dummy_tbl = pa.Table.from_pylist(dummy_lst)
    with pq.ParquetWriter(file_name, dummy_tbl.schema) as writer:
        for src in ichunked(generator, chunk_size):
            lst = {'float': []}
            for num in src:
                lst['float'].append(num)
            tbl = pa.Table.from_pydict(lst)
            writer.write_table(tbl)

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}


@click.command(context_settings=CONTEXT_SETTINGS, no_args_is_help=True)
@click.argument("folder", type=click.STRING, default="data")
@click.argument("total", type=click.INT, default=1000_000_000)
def main(folder, total):
    """Generates parquet file with random floats 

        FOLDER file to generate

        TOTAL number of records to produce

    """
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TransferSpeedColumn()
    ) as progress:
        rnd = random.Random()
        # generator = gen_random(rnd, total)
        # tracked_generator = progress.track(generator, total=total)
        # save_parquet(os.path.join(folder, 'fine.parquet'), tracked_generator, 1)
        generator = gen_random(rnd, total)
        tracked_generator = progress.track(generator, total=total)
        save_parquet(os.path.join(folder, 'chunked_10M_1B.parquet'), tracked_generator, 10_000_000)
        return 0
    
if __name__ == "__main__":
    sys.exit(main())
