import copy
from typing import Any  # noqa: F401
from typing import Dict  # noqa: F401
from typing import Iterable  # noqa: F401
from typing import List  # noqa: F401
from typing import Optional  # noqa: F401
from typing import Text  # noqa: F401

from thrift.protocol import TBinaryProtocol
from thrift.transport import TSocket
from thrift.transport import TTransport

from pdsm.hive_metastore import ThriftHiveMetastore
from pdsm.hive_metastore.ttypes import DropPartitionsRequest
from pdsm.hive_metastore.ttypes import NoSuchObjectException
from pdsm.hive_metastore.ttypes import RequestPartsSpec
from pdsm.hive_metastore.ttypes import Table as HiveTable
from pdsm.models import HIVE_STORAGE_DESCRIPTOR_TEMPLATE
from pdsm.models import Column
from pdsm.models import Partition
from pdsm.utils import chunks
from pdsm.utils import ensure_trailing_slash
from pdsm.utils import remove_trailing_slash

HIVE_TABLE_TEMPLATE = HiveTable(  # type: ignore
    tableName="",
    dbName="",
    owner="hadoop",
    sd=HIVE_STORAGE_DESCRIPTOR_TEMPLATE,
    partitionKeys=[],
    tableType="EXTERNAL_TABLE",
    parameters={
        "hive.hcatalog.partition.spec.grouping.enabled": "TRUE",
        "EXTERNAL": "TRUE",
    },
)

CONFIG = {"HOST": "127.0.0.1", "PORT": 9083}


def create_client():
    # type: () -> Any
    socket = TSocket.TSocket(CONFIG["HOST"], CONFIG["PORT"])
    transport = TTransport.TBufferedTransport(socket)
    protocol = TBinaryProtocol.TBinaryProtocol(transport)
    client = ThriftHiveMetastore.Client(protocol)  # type: ignore
    transport.open()
    return client


class Table(object):
    __slots__ = ["database_name", "name", "columns", "location", "partition_keys"]

    def __init__(self, database_name, name, columns, location, partition_keys):
        # type: (Text, Text, List[Column], Text, List[Column]) -> None
        self.database_name = database_name
        self.name = name
        self.columns = columns
        self.location = location
        self.partition_keys = partition_keys

    def list_partitions(self, batch_size=100):
        # type: (int) -> Iterable[Partition]
        client = create_client()
        names = client.get_partition_names(self.database_name, self.name, -1)
        for chunk in chunks(names, batch_size):
            result = client.get_partitions_by_names(
                self.database_name, self.name, chunk
            )
            for pd in result:
                yield Partition.from_hive(pd)

    def add_partitions(self, partitions, batch_size=100):
        # type: (List[Partition], int) -> None
        client = create_client()
        for partition_chunk in chunks(partitions, batch_size):
            new_partitions = [partition.to_hive() for partition in partition_chunk]
            for new_partition in new_partitions:
                new_partition.dbName = self.database_name
                new_partition.tableName = self.name
            client.add_partitions(new_parts=new_partitions)

    def update_partitions(self, partitions, batch_size=100):
        # type: (List[Partition], int) -> None
        client = create_client()
        for partition_chunk in chunks(partitions, 100):
            client.drop_partitions_req(
                req=DropPartitionsRequest(  # type: ignore
                    dbName=self.database_name,
                    tblName=self.name,
                    parts=RequestPartsSpec(
                        names=[
                            partition.to_name(self.partition_keys)
                            for partition in partition_chunk
                        ]
                    ),
                    deleteData=False,
                    ifExists=True,
                    needResult=False,
                )
            )
            new_partitions = [partition.to_hive() for partition in partition_chunk]
            for new_partition in new_partitions:
                new_partition.dbName = self.database_name
                new_partition.tableName = self.name
            client.add_partitions(new_parts=new_partitions)

    @classmethod
    def from_hive(cls, data):
        # type: (HiveTable) -> Table
        table = cls(
            database_name=data.dbName,
            name=data.tableName,
            columns=[Column.from_hive(cd) for cd in data.sd.cols],
            location=ensure_trailing_slash(data.sd.location),
            partition_keys=[Column.from_hive(cd) for cd in data.partitionKeys],
        )
        return table

    def to_hive(self):
        # type: () -> HiveTable
        data = copy.deepcopy(HIVE_TABLE_TEMPLATE)  # type: HiveTable
        data.tableName = self.name
        data.dbName = self.database_name
        data.sd.cols = [column.to_hive() for column in self.columns]
        data.sd.location = remove_trailing_slash(self.location)
        data.partitionKeys = [column.to_hive() for column in self.partition_keys]
        return data

    @classmethod
    def get(cls, database_name, name):
        # type: (Text, Text) -> Optional[Table]
        client = create_client()
        try:
            result = client.get_table(database_name, name)
        except NoSuchObjectException:
            return None
        return cls.from_hive(result)

    @classmethod
    def create(cls, database_name, name, columns, location, partition_keys):
        # type: (Text, Text, List[Column], Text, List[Column]) -> Table
        client = create_client()
        table = cls(
            database_name=database_name,
            name=name,
            columns=columns,
            location=location,
            partition_keys=partition_keys,
        )
        client.create_table(tbl=table.to_hive())
        return table

    @classmethod
    def update(cls, database_name, name, columns, location, partition_keys):
        # type: (Text, Text, List[Column], Text, List[Column]) -> Table
        client = create_client()
        table = cls(
            database_name=database_name,
            name=name,
            columns=columns,
            location=location,
            partition_keys=partition_keys,
        )
        client.alter_table(dbname=database_name, tbl_name=name, new_tbl=table.to_hive())
        return table

    @classmethod
    def drop(cls, database_name, name):
        # type: (Text, Text) -> None
        client = create_client()
        client.drop_table(dbname=database_name, name=name, deleteData=False)


def build_partition_name(keys, vals):
    # type: (List[Text], List[Text]) -> Text
    return "/".join("{}={}".format(keys[idx], vals[idx]) for idx in range(len(keys)))
