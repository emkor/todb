from datetime import datetime


def seconds_between(start: datetime, end: datetime = None, precision: int = 3) -> float:
    return round(((end or datetime.utcnow()) - start).total_seconds(), precision)
