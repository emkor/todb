from datetime import datetime
from os import path


def seconds_between(start: datetime, end: datetime = None, precision: int = 3) -> float:
    return round(((end or datetime.utcnow()) - start).total_seconds(), precision)


def rel_project_path_to_absolute(rel_project_path: str) -> str:
    this_file_dir = path.dirname(path.realpath(__file__))
    project_directory, project_dir_name, _ = this_file_dir.partition("todb/")
    return path.abspath(path.join(project_directory, project_dir_name, rel_project_path))
