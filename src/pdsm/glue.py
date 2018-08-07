import copy

import botocore.session
from botocore.exceptions import ClientError

from .models import STORAGE_DESCRIPTOR_TEMPLATE
from .models import Column
from .models import Partition
from .utils import chunks
from .utils import ensure_trailing_slash
from .utils import remove_trailing_slash

TABLE_INPUT_TEMPLATE = {
    'Name': '',
    'Owner': 'hadoop',
    'StorageDescriptor': STORAGE_DESCRIPTOR_TEMPLATE,
    'PartitionKeys': [],
    'TableType': 'EXTERNAL_TABLE',
    'Parameters': {'EXTERNAL': 'TRUE'},
}


class Table(object):
    __slots__ = ['database_name', 'name', 'columns', 'location', 'partition_keys']

    def __init__(self, database_name, name, columns, location, partition_keys):
        self.database_name = database_name
        self.name = name
        self.columns = columns
        self.location = location
        self.partition_keys = partition_keys

    def list_partitions(self):
        client = botocore.session.get_session().create_client('glue')
        opts = {'DatabaseName': self.database_name, 'TableName': self.name}
        while True:
            result = client.get_partitions(**opts)
            if 'Partitions' in result:
                for pd in result['Partitions']:
                    yield Partition.from_input(pd)
            if 'NextToken' in result:
                opts['NextToken'] = result['NextToken']
            else:
                break

    def get_partitions(self):
        client = botocore.session.get_session().create_client('glue')
        opts = {'DatabaseName': self.database_name, 'TableName': self.name}
        partitions = []
        while True:
            result = client.get_partitions(**opts)
            if 'Partitions' in result:
                partitions += [Partition.from_input(pd) for pd in result['Partitions']]
            if 'NextToken' in result:
                opts['NextToken'] = result['NextToken']
            else:
                break
        return partitions

    def add_partitions(self, partitions):
        client = botocore.session.get_session().create_client('glue')
        for partition_chunk in chunks(partitions, 100):
            data = {'DatabaseName': self.database_name,
                    'TableName': self.name,
                    'PartitionInputList': [partition.to_input() for partition in partition_chunk]}
            client.batch_create_partition(**data)

    def recreate_partitions(self, partitions):
        client = botocore.session.get_session().create_client('glue')
        for partition_chunk in chunks(partitions, 25):
            data = {'DatabaseName': self.database_name,
                    'TableName': self.name,
                    'PartitionsToDelete': [{'Values': partition.values} for partition in partition_chunk]}
            client.batch_delete_partition(**data)
            data = {'DatabaseName': self.database_name,
                    'TableName': self.name,
                    'PartitionInputList': [partition.to_input() for partition in partition_chunk]}
            client.batch_create_partition(**data)

    @classmethod
    def from_input(cls, database_name, data):
        table = cls(
            database_name=database_name,
            name=data['Name'],
            columns=[Column.from_input(cd) for cd in data['StorageDescriptor']['Columns']],
            location=ensure_trailing_slash(data['StorageDescriptor']['Location']),
            partition_keys=[Column.from_input(cd) for cd in data['PartitionKeys']],
        )
        return table

    def to_input(self):
        data = copy.deepcopy(TABLE_INPUT_TEMPLATE)
        data['Name'] = self.name
        data['StorageDescriptor']['Columns'] = [column.to_input() for column in self.columns]
        data['StorageDescriptor']['Location'] = remove_trailing_slash(self.location)
        data['PartitionKeys'] = [column.to_input() for column in self.partition_keys]
        return data

    @classmethod
    def get(cls, database_name, name):
        client = botocore.session.get_session().create_client('glue')
        try:
            result = client.get_table(DatabaseName=database_name, Name=name)
        except ClientError as ex:
            if ex.response['Error']['Code'] == 'EntityNotFoundException':
                return None
            raise ex
        return cls.from_input(database_name, result['Table'])

    @classmethod
    def create(cls, database_name, name, columns, location, partition_keys):
        client = botocore.session.get_session().create_client('glue')
        table = cls(
            database_name=database_name,
            name=name,
            columns=columns,
            location=location,
            partition_keys=partition_keys,
        )
        client.create_table(
            DatabaseName=database_name,
            TableInput=table.to_input(),
        )
        return table

    @classmethod
    def update(cls, database_name, name, columns, location, partition_keys):
        client = botocore.session.get_session().create_client('glue')
        table = cls(
            database_name=database_name,
            name=name,
            columns=columns,
            location=location,
            partition_keys=partition_keys,
        )
        client.update_table(
            DatabaseName=database_name,
            TableInput=table.to_input(),
        )
        return table

    @classmethod
    def drop(cls, database_name, name):
        client = botocore.session.get_session().create_client('glue')
        client.delete_table(
            DatabaseName=database_name,
            Name=name,
        )
