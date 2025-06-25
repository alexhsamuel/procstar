import datetime

#-------------------------------------------------------------------------------

def now() -> datetime.datetime:
    """
    Returns the current time as an explicit UTC `datetime`.
    """
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=datetime.timezone.utc)


