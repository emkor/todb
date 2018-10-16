from datetime import datetime
from os import path
from typing import Optional, Any, List, Dict
from uuid import UUID, uuid5

SEED_TEXT = UUID(int=1248789574)


def seconds_between(start: datetime, end: datetime = None, precision: int = 3) -> float:
    return round(((end or datetime.utcnow()) - start).total_seconds(), precision)


def proj_path_to_abs(rel_project_path: str) -> str:
    this_file_dir = path.dirname(path.realpath(__file__))
    project_directory, project_dir_name, _ = this_file_dir.partition("todb/")
    return path.abspath(path.join(project_directory, project_dir_name, rel_project_path))


def limit_or_default(value: Optional[int], default: int,
                     lower_bound: Optional[int] = None, upper_bound: Optional[int] = None) -> int:
    if value is None:
        return default
    else:
        if lower_bound is not None and upper_bound is not None:
            return min(max(value, lower_bound), upper_bound)
        elif lower_bound is not None:
            return max(value, lower_bound)
        elif upper_bound is not None:
            return min(value, upper_bound)
        else:
            raise ValueError("Did not provide lower bound and upper bound!")


def gen_uuid_from_obj(o: Dict[str, Any], sorted_keys: List[str]) -> str:
    cells_as_str = ";".join([str(o[c]) for c in sorted_keys])
    return gen_uuid(str(cells_as_str))


def gen_uuid(content: str) -> str:
    return str(uuid5(SEED_TEXT, str(content)))

print(gen_uuid("some text alalal"))