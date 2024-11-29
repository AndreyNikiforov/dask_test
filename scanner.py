
# pip install "dask[complete]" click

import dask.dataframe as dd
import click
import sys
from rich.progress import Progress, TextColumn, BarColumn, TaskProgressColumn, TimeElapsedColumn, TimeRemainingColumn,TransferSpeedColumn

CONTEXT_SETTINGS = {"help_option_names": ["-h", "--help"]}

@click.command(context_settings=CONTEXT_SETTINGS, no_args_is_help=True)
@click.argument("path", type=click.STRING)
def main(path):
    """Scans records in all parquet files matching pattern

        PATH file pattern to files, e.g. "data/lng-lat-tz/geo*.parquet"
    """

    df = dd.read_parquet(
        path, 
        )
    with Progress(
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TransferSpeedColumn()
            ) as progress:
        count = 0
        for _ in progress.track(df.itertuples(True, None)):
            count = count + 1
        print(f"Total records: {count}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
