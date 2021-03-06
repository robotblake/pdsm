import logging
import time
from typing import Optional  # noqa: F401
from typing import Text      # noqa: F401

import click

from .dataset import Dataset
from .dataset import get_datasets
from .dataset import get_versions
from .glue import Table
from .utils import ensure_trailing_slash
from .utils import underscore

logging.basicConfig(
    format='time="%(asctime)s" level=%(levelname)s name=%(name)s msg="%(message)s"',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    level=logging.INFO,
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)


def run(src, version=None, alias=None):
    # type: (Text, Optional[Text], Optional[Text]) -> None
    src = ensure_trailing_slash(src)

    if version:
        location = u'{}{}/'.format(src, version)
    else:
        locations = sorted(get_versions(src))
        if not locations:
            return
        location = locations[-1]

    logger.info('Loading dataset from %s', location)
    dataset = Dataset.get(location)
    if dataset is None:
        logger.info('Skipping %s, no parquet files found', location)
        return

    logger.info('Started processing %s', location)

    table_names = [underscore(alias or dataset.name) + '_' + dataset.version]
    if not version:
        table_names.append(underscore(alias or dataset.name))

    for table_name in table_names:
        table = Table.get('telemetry', table_name)

        if not table:
            logger.info('Creating %s', table_name)
            table = Table.create(
                database_name='telemetry',
                name=table_name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        elif table.location != dataset.location:
            logger.info('Recreating %s', table_name)
            Table.drop('telemetry', table.name)
            table = Table.create(
                database_name='telemetry',
                name=table_name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        elif set(dataset.columns) != set(table.columns):
            logger.info('Updating %s', table_name)
            table = Table.update(
                database_name=table.database_name,
                name=table.name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        columns_set = set(dataset.columns)

        different = []
        missing = set(dataset.partitions)

        for table_partition in table.list_partitions():
            if columns_set != set(table_partition.columns):
                table_partition.columns = dataset.columns
                different.append(table_partition)

            missing.discard(table_partition)

            if len(different) == 100:
                logger.info('Recreating %d partitions on %s', len(different), table_name)
                table.recreate_partitions(different)
                different = []

        if different:
            logger.info('Recreating %d partitions on %s', len(different), table_name)
            table.recreate_partitions(different)

        if missing:
            logger.info('Adding %d partitions to %s', len(missing), table_name)
            table.add_partitions(sorted(missing))

    logger.info('Finished processing %s', location)


@click.command()
@click.argument('src')
@click.option('--version')
@click.option('--alias')
@click.option('--discover', is_flag=True)
def main(src, version, alias, discover):
    # type: (Text, Text, Text, bool) -> None
    if discover:
        for location in get_datasets(src):
            run(src=location)
    else:
        run(src=src, version=version, alias=alias)
