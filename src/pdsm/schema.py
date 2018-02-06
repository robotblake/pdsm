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

    if magic_number != b'PAR1':
        raise ParquetError('magic number is invalid')

    offset = offset - footer_size
    response = client.get_object(Bucket=bucket, Key=key, Range='bytes={}-'.format(offset))

    transport = TTransport.TFileObjectTransport(response['Body'])
    protocol = TCompactProtocol.TCompactProtocol(transport)
    metadata = FileMetaData()
    metadata.read(protocol)

    return metadata


def to_columns(schema):
    columns = []
    context = []

    column_name = ''
    column_type = ''

    # Set to "true" to skip the first element
    skip_iteration = True
    ignore_repetition = False

    for idx, element in enumerate(schema):
        # Skip iteration if skip was set
        if skip_iteration:
            skip_iteration = False
            continue

        # If there's no current context, set current
        if len(context) == 0:
            column_name = element.name.lower()
            column_type = ''
        else:
            # If we're in a group but not the first member, add a comma
            if column_type[-1] != '<':
                column_type += ','

            # If we're in a struct, append the name to the type
            if context[-1][0] == 0:
                column_type += element.name.lower() + ':'

            # Decrement context
            context[-1][1] -= 1

        # Group Type
        if element.type is None:
            # List Type
            if element.converted_type == 3:
                child = schema[idx+1]

                # If child is not a group, or has more than 1 child, or is
                # named "array" or "<parent.name>_tuple", than ignore the
                # "repetition type" of the next element
                if (child.type is not None or child.num_children > 1
                        or child.name in ('array', element.name + '_tuple')):
                    ignore_repetition = True

                # Otherwise skip the next element
                skip_iteration = not ignore_repetition

                context.append([2, 1])
                column_type += 'array<'

            # Map Type
            elif element.converted_type in (1, 2):
                # Always skip next element
                skip_iteration = True
                context.append([1, 2])
                column_type += 'map<'

            # Struct Type
            else:
                context.append([0, element.num_children])
                column_type += 'struct<'

            # Skip rest of iteration
            continue

        # List Type (Unannotated Repeated)
        if element.repetition_type == 2:
            if not ignore_repetition:
                context.append([2, 0])
                column_type += 'array<'
            else:
                ignore_repetition = False

        # String Type
        if element.type == 6 and element.converted_type in (None, 0):
            column_type += 'string'

        # Decimal Type
        elif element.type == 7 and element.converted_type == 5:
            column_type += 'decimal({},{})'.format(element.precision, element.scale)

        # Simple Type
        elif element.type in TYPE_MAP:
            column_type += TYPE_MAP[element.type]

        # Unknown Type
        else:
            raise ParquetError('unknown element type {}'.format(element))

        # Unwind context until empty or it's last count is > 0
        while len(context) > 0 and context[-1][1] == 0:
            column_type += '>'
            context.pop()

        # If context is empty, we're back at root
        if len(context) == 0:
            columns.append(Column(column_name, column_type))

    return columns
