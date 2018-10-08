from typing import Dict, List, Any, Optional

from datetime import datetime, date, time

from dateutil.parser import parse
from todb.data_types import ConfColumn

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


class NullInRequiredColumn(ValueError):
    pass


class EntityBuilder(object):
    def __init__(self, columns: List[ConfColumn]) -> None:
        self.columns = columns

    def to_entity(self, cells_in_row: List[str]) -> Optional[Dict[str, Any]]:
        if len(self.columns) < len(cells_in_row):
            try:
                return {c.name: self._cast_value_to_sql_compatible(c, cells_in_row[c.col_index]) for c in self.columns}
            except NullInRequiredColumn as e:
                print("Can not build entity from row {}: {}".format(cells_in_row, e))
                return None
        else:
            print("Row {} has less cells ({}) then defined columns ({})".format(cells_in_row,
                                                                                len(cells_in_row),
                                                                                len(self.columns)))
            return None

    def _cast_value_to_sql_compatible(self, column: ConfColumn, value: Optional[str]) -> Optional[Any]:
        if not value:
            if column.nullable:
                return None
            else:
                raise NullInRequiredColumn("Value for column {} is empty!".format(column.name))
        try:
            if column.python_type in (datetime, date, time):
                parsed_time = parse(value)
                if column.python_type == datetime:
                    return parsed_time
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
            print("WARNING: Could not cast {} for column {}: {}".format(value, column, e))
            if column.nullable:
                return None
            else:
                raise e
