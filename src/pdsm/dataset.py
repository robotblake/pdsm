import datetime
import re
from functools import total_ordering

import botocore.session
from dateutil.tz import tzutc

from .models import Column
from .models import Partition
from .schema import read_metadata
from .schema import to_columns
from .utils import ensure_trailing_slash
from .utils import split_s3_bucket_key

IGNORED_MATCHER = re.compile(r'''(?:.*/)?(?:
    _spark_metadata/  # spark metadata directory
  | _common_metadata$ # parquet metadata file
  | _metadata$        # parquet metadata file
  | _temporary/       # spark temporary directory
  | [^/]_\$folder\$$  # hadoop folder marker
  | /$                # directory representation

  # this next filter is here for historical reasons
  | _[^=/]*(/|$)      # other temporary directoy
)''', re.VERBOSE)

NAME_VERSION = re.compile(r'([^/]+)(?:/(v[0-9]+))?/$')

DATASET_MATCHER = re.compile(r'([a-z](?:[_-]?[a-z0-9]+)*)', re.IGNORECASE)

VERSION_MATCHER = re.compile(r'(v[0-9]+)/$')

PARTITION_MATCHER = re.compile(r'([^=/]+=[^=/]+(?:/[^=/]+=[^=/]+)*)/')

UNIX_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=tzutc())


def get_datasets(location):
    location = ensure_trailing_slash(location)
    bucket, prefix = split_s3_bucket_key(location)
    iterator = get_iterator(bucket, prefix, '/', 'CommonPrefixes[].Prefix')
    for result in iterator:
        matches = DATASET_MATCHER.match(result, len(prefix))
        if not matches:
            continue
        yield 's3://{}/{}'.format(bucket, result)


def get_versions(location):
    location = ensure_trailing_slash(location)
    bucket, prefix = split_s3_bucket_key(location)
    iterator = get_iterator(bucket, prefix, '/', 'CommonPrefixes[].Prefix')
    for result in iterator:
        matches = VERSION_MATCHER.match(result, len(prefix))
        if not matches:
            continue
        yield 's3://{}/{}'.format(bucket, result)


def get_iterator(bucket, prefix, delimiter=None, search=None):
    client = botocore.session.get_session().create_client('s3')
    paginator = client.get_paginator('list_objects_v2')
    options = {'Bucket': bucket, 'Prefix': prefix}
    if delimiter:
        options['Delimiter'] = delimiter
    iterator = paginator.paginate(**options)
    if search:
        iterator = iterator.search(search)
    return (result for result in iterator if result is not None)


def get_object_summaries(bucket, prefix):
    summaries = []
    for result in get_iterator(bucket, prefix, search='Contents[]'):
        if IGNORED_MATCHER.match(result['Key']):
            continue
        if result['Size'] < 12:
            continue
        if '=__HIVE_DEFAULT_PARTITION__/' in result['Key']:
            continue
        summaries.append(result)
    return sorted(summaries, key=lambda x: x['LastModified'])


def list_object_summaries(bucket, prefix):
    for result in get_iterator(bucket, prefix, search='Contents[]'):
        if IGNORED_MATCHER.match(result['Key']):
            continue
        if result['Size'] < 12:
            continue
        if '=__HIVE_DEFAULT_PARTITION__/' in result['Key']:
            continue
        yield result


@total_ordering
class Dataset(object):
    __slots__ = ['name', 'version', 'columns', 'partitions', 'location', 'partition_keys']

    def __init__(self, name, version, columns, partitions, location, partition_keys):
        self.name = name
        self.version = version
        self.columns = columns
        self.partitions = partitions
        self.location = location
        self.partition_keys = partition_keys

    @classmethod
    def get(cls, location):
        location = ensure_trailing_slash(location)
        bucket, prefix = split_s3_bucket_key(location)
        name, version = NAME_VERSION.search(prefix).groups()

        # get latest object and partition names
        latest = None
        partition_names = set()
        for summary in list_object_summaries(bucket, prefix):
            if not latest or summary['LastModified'] > latest['LastModified']:
                latest = summary
            matches = PARTITION_MATCHER.match(summary['Key'], len(prefix))
            if matches:
                partition_names.add(matches.group(1))
        if latest is None:
            return None
        partition_names = sorted(partition_names)

        # read columns from object
        metadata = read_metadata(bucket, latest['Key'], latest['Size'])
        columns = to_columns(metadata.schema)

        # get partition keys from last partition
        partition_keys = []
        if partition_names:
            partition_keys = [Column(p.split('=')[0], 'string') for p in partition_names[-1].split('/')]

        # create partition objects
        partitions = []
        for partition_name in partition_names:
            partition = Partition(
                values=[p.split('=')[1] for p in partition_name.split('/')],
                columns=columns,
                location='{}{}/'.format(location, partition_name),
            )
            partitions.append(partition)

        dataset = cls(
            name=name,
            version=version,
            columns=columns,
            partitions=partitions,
            location=location,
            partition_keys=partition_keys,
        )

        return dataset

    def __eq__(self, other):
        return self.location == other.location

    def __lt__(self, other):
        return int(self.name[1:]) < int(other.name[1:])
