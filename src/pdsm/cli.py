import click

from .dataset import Dataset
from .dataset import get_datasets
from .dataset import get_versions
from .glue import Table
from .utils import ensure_trailing_slash
from .utils import underscore


def run(src, version, alias):
    src = ensure_trailing_slash(src)

    if version:
        location = '{}{}/'.format(src, version)
    else:
        locations = sorted(get_versions(src))
        if not locations:
            return
        location = locations[-1]
    dataset = Dataset.get(location)

    table_names = [underscore(alias or dataset.name) + '_' + dataset.version,
                   underscore(alias or dataset.name)]

    for table_name in table_names:
        table = Table.get('telemetry', table_name)

        if not table:
            print('Creating {}'.format(table_name))
            table = Table.create(
                database_name='telemetry',
                name=table_name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        elif table.location != dataset.location:
            print('Recreating {}'.format(table.name))
            Table.drop('telemetry', table.name)
            table = Table.create(
                database_name='telemetry',
                name=table_name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        elif set(dataset.columns) - set(table.columns):
            print('Updating {}'.format(table.name))
            table = Table.update(
                database_name=table.database_name,
                name=table.name,
                columns=dataset.columns,
                location=dataset.location,
                partition_keys=dataset.partition_keys,
            )

        table_partitions = table.get_partitions()
        different = []
        for table_partition in table_partitions:
            if set(dataset.columns) - set(table_partition.columns):
                table_partition.columns = dataset.columns
                different.append(table_partition)
        missing = sorted(set(dataset.partitions) - set(table_partitions))
        if different:
            print('Recreating {} partitions on {}'.format(len(different), table.name))
            table.recreate_partitions(different)
        if missing:
            print('Adding {} partitions to {}'.format(len(missing), table.name))
            table.add_partitions(missing)


@click.command()
@click.argument('src')
@click.option('--version')
@click.option('--alias')
@click.option('--discover', is_flag=True)
def main(src, version, alias, discover):
    if discover:
        for location in get_datasets(src):
            run(src=location, version=None, alias=None)
    else:
        run(src=src, version=version, alias=alias)
