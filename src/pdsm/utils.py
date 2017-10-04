import re


def ensure_trailing_slash(mystr):
    if not mystr.endswith('/'):
        mystr = mystr + '/'
    return mystr


def remove_trailing_slash(mystr):
    if mystr.endswith('/'):
        mystr = mystr[:-1]
    return mystr


def split_s3_bucket_key(s3_path):
    if s3_path.startswith('s3://'):
        s3_path = s3_path[5:]
    parts = s3_path.split('/')
    return parts[0], '/'.join(parts[1:])


def underscore(mystr):
    mystr = re.sub(r'([A-Z]+)([A-Z][a-z])', r'\1_\2', mystr)
    mystr = re.sub(r'([a-z\d])([A-Z])', r'\1_\2', mystr)
    mystr = mystr.replace('-', '_')
    return mystr.lower()


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]
