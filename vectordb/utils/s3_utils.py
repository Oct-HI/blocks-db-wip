import re

EXPRESS_PATTERN = re.compile(r"--([a-z0-9]+-[a-z]+[0-9]+)--x-s3$")


def is_s3express_bucket(bucket: str) -> bool:
    return bool(EXPRESS_PATTERN.search(bucket))


def parse_express_az(bucket: str) -> str | None:
    m = EXPRESS_PATTERN.search(bucket)
    return m.group(1) if m else None
