import struct

import botocore.session
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

from .models import Column
from .parquet.ttypes import FileMetaData

TYPE_MAP = {
    0: 'boolean',    # boolean
    1: 'int',        # int32
    2: 'bigint',     # int64
    3: 'timestamp',  # int96
    4: 'float',      # float
    5: 'double',     # double
    6: 'binary',     # byte_array
}


class ParquetError(Exception):
    pass


def read_metadata(bucket, key, size):
    session = botocore.session.get_session()
    client = session.create_client('s3')

    offset = size - 8
    response = client.get_object(Bucket=bucket, Key=key, Range='bytes={}-'.format(offset))
    footer_size = struct.unpack('<i', response['Body'].read(4))[0]
    magic_number = response['Body'].read(4)

    if size < (12 + footer_size):
        raise ParquetError('file is too small')

    if magic_number != 'PAR1':
        raise ParquetError('magic number is invalid')

    offset = offset - footer_size
    response = client.get_object(Bucket=bucket, Key=key, Range='bytes={}-'.format(offset))

    transport = TTransport.TFileObjectTransport(response['Body'])
    protocol = TCompactProtocol.TCompactProtocol(transport)
    metadata = FileMetaData()
    metadata.read(protocol)

    return metadata


class Node(object):
    def __init__(self, data, child, sibling):
        self.data = data
        self.child = child
        self.sibling = sibling

    @property
    def children(self):
        children = []
        child = self.child
        while child:
            children.append(child)
            child = child.sibling
        return children

    @property
    def name(self):
        return self.data.name

    @property
    def type(self):
        return self.data.type

    @property
    def converted_type(self):
        return self.data.converted_type

    @property
    def repetition_type(self):
        return self.data.repetition_type

    @property
    def num_children(self):
        return self.data.num_children

    @property
    def precision(self):
        return self.data.precision

    @property
    def scale(self):
        return self.data.scale

    def set_required(self):
        self.data.repetition_type = 0

    def is_group(self):
        return self.type is None

    def is_list(self):
        return self.is_group() and self.converted_type == 3

    def is_map(self):
        return self.is_group() and self.converted_type in (1, 2)

    def is_struct(self):
        return self.is_group() and self.converted_type is None

    def is_repeated(self):
        return self.repetition_type == 2

    def is_string(self):
        # return self.type == 6 and self.converted_type == 0
        return self.type == 6 and self.converted_type in (None, 0)

    def is_decimal(self):
        return self.type == 7 and self.converted_type == 5


def to_columns(schema):
    root = to_tree(schema)
    columns = [Column(child.name.lower(), hive_type(child))
               for child in root.children]
    return columns


def to_tree(schema):
    if not schema:
        return None

    offset = 0
    for idx, elem in enumerate(schema):
        if elem.type is None:
            offset += elem.num_children
        if idx == offset:
            break

    data = schema[0]
    left = schema[1:offset+1]
    right = schema[offset+1:]
    return Node(data, to_tree(left), to_tree(right))


def hive_type(node):
    converted = None

    # list type
    if node.is_list():
        child = node.child
        # if the repeated field is not a group, than its type is the element type and elements are
        # required
        if not child.is_group():
            child.set_required()
            converted = 'array<{}>'.format(hive_type(child))

        # if the repeated field is a group with multiple fields, than its type is the element type
        # and elements are required
        elif child.is_group() and child.num_children > 1:
            child.set_required()
            converted = 'array<{}>'.format(hive_type(child))

        # if the repeated field is a group with one field and is named either array or uses the
        # LIST-annotated group's name with _tuple appended then the repeated type is the element
        # type and elements are required
        elif (child.is_group() and child.num_children == 1
              and child.name in ('array', node.name + '_tuple')):
            child.set_required()
            converted = 'array<{}>'.format(hive_type(child))

        else:
            converted = 'array<{}>'.format(hive_type(child.child))

    # map type
    elif node.is_map():
        child = node.child.child
        key = hive_type(child)
        val = hive_type(child.sibling)
        converted = 'map<{},{}>'.format(key, val)

    # struct type
    elif node.is_struct():
        subs = ['{}:{}'.format(child.name, hive_type(child)) for child in node.children]
        converted = 'struct<{}>'.format(','.join(subs))

    # unannotated repeated type
    elif node.is_repeated():
        node.set_required()
        converted = 'array<{}>'.format(hive_type(node))

    # byte_array type + utf8 converted_type = string
    elif node.is_string():
        converted = 'string'

    # decimal type
    elif node.is_decimal():
        converted = 'decimal({},{})'.format(node.precision, node.scale)

    # conversion map
    elif node.type in TYPE_MAP:
        converted = TYPE_MAP[node.type]

    return converted
