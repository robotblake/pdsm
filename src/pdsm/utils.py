import re
from typing import Any  # noqa: F401
from typing import Iterable  # noqa: F401
from typing import List  # noqa: F401
from typing import Text  # noqa: F401
from typing import Tuple  # noqa: F401


def ensure_trailing_slash(mystr):
    # type: (Text) -> Text
    if not mystr.endswith("/"):
        mystr = mystr + "/"
    return mystr


def remove_trailing_slash(mystr):
    # type: (Text) -> Text
    if mystr.endswith("/"):
        mystr = mystr[:-1]
    return mystr


def split_s3_bucket_key(s3_path):
    # type: (Text) -> Tuple[Text, Text]
    if s3_path.startswith("s3://"):
        s3_path = s3_path[5:]
    parts = s3_path.split("/")
    return parts[0], "/".join(parts[1:])


def underscore(mystr):
    # type: (Text) -> Text
    mystr = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", mystr)
    mystr = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", mystr)
    mystr = mystr.replace("-", "_")
    return mystr.lower()


def chunks(l, n):
    # type: (List[Any], int) -> Iterable[List[Any]]
    for i in range(0, len(l), n):
        yield l[i : i + n]
