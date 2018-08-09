import copy
from functools import total_ordering
from typing import Any   # noqa: F401
from typing import Dict  # noqa: F401
from typing import List  # noqa: F401
from typing import Text  # noqa: F401

import botocore.session

from .utils import ensure_trailing_slash
from .utils import remove_trailing_slash

STORAGE_DESCRIPTOR_TEMPLATE = {
    'Columns': [],
    'Location': '',
    'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
    'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
    'Compressed': False,
    'NumberOfBuckets': -1,
    'SerdeInfo': {
        'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
        'Parameters': {'serialization.format': '1'},
    },
    'BucketColumns': [],
    'SortColumns': [],
    'Parameters': {},
    'SkewedInfo': {
        'SkewedColumnNames': [],
        'SkewedColumnValues': [],
        'SkewedColumnValueLocationMaps': {},
    },
    'StoredAsSubDirectories': False,
}

PARTITION_INPUT_TEMPLATE = {
    'Values': [],
    'StorageDescriptor': STORAGE_DESCRIPTOR_TEMPLATE,
}  # type: Dict[Text, Any]


class Column(object):
    __slots__ = ['name', 'type']

    def __init__(self, name, type_):
        # type: (Text, Text) -> None
        self.name = name
        self.type = type_

    @classmethod
    def from_input(cls, data):
        # type: (Dict[Text, Text]) -> Column
        column = cls(
            name=data['Name'],
            type_=data['Type'],
        )
        return column

    def to_input(self):
        # type: () -> Dict[Text, Text]
        data = {u'Name': self.name, u'Type': self.type}
        return data

    def __eq__(self, other):
        # type: (object) -> bool
        if not isinstance(other, Column):
            return NotImplemented
        return (self.name, self.type) == (other.name, other.type)

    def __hash__(self):
        # type: () -> int
        return hash((self.name, self.type))

    def __repr__(self):
        # type: () -> str
        return 'Column(name={}, type={})'.format(self.name, self.type)


@total_ordering
class Partition(object):
    __slots__ = ['values', 'columns', 'location']

    def __init__(self, values, columns, location):
        # type: (List[Text], List[Column], Text) -> None
        self.values = values
        self.columns = columns
        self.location = location

    @classmethod
    def from_input(cls, data):
        # type: (Dict[Text, Any]) -> Partition
        partition = cls(
            values=data['Values'],
            columns=[Column.from_input(cd) for cd in data['StorageDescriptor']['Columns']],
            location=ensure_trailing_slash(data['StorageDescriptor']['Location']),
        )
        return partition

    def to_input(self):
        # type: () -> Dict[Text, Any]
        data = copy.deepcopy(PARTITION_INPUT_TEMPLATE)
        data['Values'] = self.values
        data['StorageDescriptor']['Columns'] = [column.to_input() for column in self.columns]
        data['StorageDescriptor']['Location'] = remove_trailing_slash(self.location)
        return data

    @classmethod
    def get(cls, database_name, table_name, values):
        # type: (Text, Text, List[Text]) -> Partition
        client = botocore.session.get_session().create_client('glue')
        result = client.get_partition(
            DatabaseName=database_name,
            TableName=table_name,
            PartitionValues=values,
        )
        return cls.from_input(result['Partition'])

    def __eq__(self, other):
        # type: (object) -> bool
        if not isinstance(other, Partition):
            return NotImplemented
        return self.location == other.location

    def __lt__(self, other):
        # type: (object) -> bool
        if not isinstance(other, Partition):
            return NotImplemented
        return self.location == other.location

    def __hash__(self):
        # type: () -> int
        return hash(self.location)

    def __repr__(self):
        # type: () -> str
        return 'Partition(location={})'.format(self.location)
