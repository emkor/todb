from datetime import datetime
from os import path
from typing import Optional


def seconds_between(start: datetime, end: datetime = None, precision: int = 3) -> float:
    return round(((end or datetime.utcnow()) - start).total_seconds(), precision)


def proj_path_to_abs(rel_project_path: str) -> str:
    this_file_dir = path.dirname(path.realpath(__file__))
    project_directory, project_dir_name, _ = this_file_dir.partition("todb/")
    return path.abspath(path.join(project_directory, project_dir_name, rel_project_path))


def limit_or_default(value: int, default: Optional[int] = None,
                     lower_bound: Optional[int] = None, upper_bound: Optional[int] = None) -> int:
    if not isinstance(value, int):
        if default is not None:
            return default
        else:
            raise ValueError("Value of {} is not an int and default was not provided!".format(value))
    if lower_bound and upper_bound:
        return min(max(value, lower_bound), upper_bound)
    elif lower_bound:
        return max(value, lower_bound)
    else:
        return min(value, upper_bound)
