from datetime import datetime, date, time
from typing import Dict, List, Any, Optional

from dateutil.parser import parse

from todb.data_model import ConfColumn, handle_lat_lon, handle_float
from todb.logger import get_logger

BOOLEAN_MAPPINGS = {
    "true": True,
    "false": False,
    "0": False,
    "yes": True,
    "no": False,
    "y": True,
    "n": False,
    "present": True,
    "absent": False
}


class EntityBuilder(object):
    def __init__(self, columns: List[ConfColumn]) -> None:
        self.columns = columns
        self.logger = get_logger()

    def to_entity(self, cells_in_row: List[str]) -> Optional[Dict[str, Any]]:
        try:
            return {c.name: self._cast_value_to_sql_compatible(c, cells_in_row[
                c.col_index] if c.col_index is not None else None)
                    for c in self.columns}
        except Exception as e:
            self.logger.debug("Can not build entity from row {}: {}".format(cells_in_row, e))
            return None

    def _cast_value_to_sql_compatible(self, column: ConfColumn, value: Optional[str]) -> Optional[Any]:
        if not value or value.lower() == "null":
            if column.nullable:
                return None
            else:
                raise ValueError("Value for column {} is empty!".format(column.name))
        try:
            if column.conf_type == "latlon":
                return handle_lat_lon(value)
            if column.conf_type == "float":
                return handle_float(value)
            elif column.python_type in (datetime, date, time):
                parsed_time = parse(value)
                if column.python_type == datetime:
                    return parsed_time.replace(tzinfo=None)
                elif column.python_type == date:
                    return parsed_time.date()
                else:
                    return parsed_time.time()
            elif column.python_type == bool:
                mapping = BOOLEAN_MAPPINGS.get(value.lower(), None)
                if mapping is not None:
                    return mapping
                else:
                    return bool(int(value))
            else:
                return column.python_type(value)
        except Exception as e:
            self.logger.warning("WARNING: Could not cast {} for column {}: {}".format(value, column, e))
            if column.nullable:
                return None
            else:
                raise e
