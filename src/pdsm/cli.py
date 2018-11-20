import logging
import time
from typing import Optional  # noqa: F401
from typing import Text  # noqa: F401

import click

from pdsm.dataset import Dataset
from pdsm.dataset import get_datasets
from pdsm.dataset import get_versions
from pdsm.glue import Table as GlueTable
from pdsm.hive import CONFIG
from pdsm.hive import Table as HiveTable
from pdsm.utils import ensure_trailing_slash
from pdsm.utils import underscore

logging.basicConfig(
    format='time="%(asctime)s" level=%(levelname)s name=%(name)s msg="%(message)s"',
    datefmt="%Y-%m-%dT%H:%M:%SZ",
    level=logging.INFO,
)
logging.Formatter.converter = time.gmtime
logger = logging.getLogger(__name__)


def run(src, version=None, alias=None, database="default", runner="glue"):
    # type: (Text, Optional[Text], Optional[Text], Text, Text) -> None
    if runner == "glue":
        Table = GlueTable
    elif runner == "hive":
        Table = HiveTable  # type: ignore
    else:
        logger.error("Invalid runner specified")
        return

    src = ensure_trailing_slash(src)

    if version:
        location = u"{}{}/".format(src, version)
    else:
        locations = sorted(get_versions(src))
        if not locations:
            return
        location = locations[-1]

    logger.info("Loading dataset from %s", location)
    dataset = Dataset.get(location)
    if dataset is None:
        logger.info("Skipping %s, no parquet files found", location)
        return

    logger.info("Started processing %s", location)

    table_names = [underscore(alias or dataset.name) + "_" + dataset.version]
    if not version:
        table_names.append(underscore(alias or dataset.name))

    for table_name in table_names:
        table = Table.get(database, table_name)

        if not table:
            logger.info("Creating %s", table_name)
            table = Table.create(
                database_name=database,
                name=table_name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        elif table.location != dataset.location:
            logger.info("Recreating %s", table_name)
            Table.drop(database, table.name)
            table = Table.create(
                database_name=database,
                name=table_name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        elif set(dataset.columns) != set(table.columns):
            logger.info("Updating %s", table_name)
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

        add_partition_batch_size = 100
        if runner == "glue":
            add_partition_batch_size = 50

        for table_partition in table.list_partitions():
            if columns_set != set(table_partition.columns):
                table_partition.columns = dataset.columns
                different.append(table_partition)

            missing.discard(table_partition)

            if len(different) == 100:
                logger.info("Updating %d partitions on %s", len(different), table_name)
                table.update_partitions(different)
                different = []

        if different:
            logger.info("Updating %d partitions on %s", len(different), table_name)
            table.update_partitions(different)

        if missing:
            logger.info("Adding %d partitions to %s", len(missing), table_name)
            table.add_partitions(sorted(missing), add_partition_batch_size)

    logger.info("Finished processing %s", location)


@click.command()
@click.argument("src")
@click.option("--version")
@click.option("--alias")
@click.option("--discover", is_flag=True)
@click.option("--hive")
@click.option("--database", default="telemetry", show_default=True)
def main(src, version, alias, discover, hive, database):
    # type: (Text, Text, Text, bool, Text, Text) -> None
    runner = "glue"
    if hive:
        if ":" in hive:
            hostport = hive.split(":")
            CONFIG["HOST"] = hostport[0]
            CONFIG["PORT"] = int(hostport[1])
        runner = "hive"

    if discover:
        for location in get_datasets(src):
            run(src=location, database=database, runner=runner)
    else:
        run(src=src, version=version, alias=alias, database=database, runner=runner)
