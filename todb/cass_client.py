from typing import List, Any, Dict
from uuid import UUID, uuid5

from cassandra.cluster import Cluster

from todb.data_model import ConfColumn, PrimaryKeyConf
from todb.db_client import DbClient

MAX_STATEMENTS_IN_BATCH = 65536
SEED_TEXT = UUID(int=1248789574)
ID_COLUMN = ConfColumn("id", -1, "string", nullable=False, indexed=True, unique=True)
KEY_SPACE = "todb"


class CassandraClient(DbClient):
    def __init__(self, cass_host: str, cass_native_port: int, compression: bool = True) -> None:
        self.cass_host = cass_host
        self.cass_native_port = cass_native_port
        self.compression = compression
        self._client = Cluster([self.cass_host], port=self.cass_native_port, compression=self.compression)

    def init_table(self, name: str, columns: List[ConfColumn], pkey: PrimaryKeyConf) -> None:
        print("Creating keyspace named todb...")
        connection = self._client.connect()
        connection.execute(
            "CREATE KEYSPACE IF NOT EXISTS todb WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };")
        connection.set_keyspace("todb")

        print("Creating table named {} with {} columns...".format(name, len(columns)))
        column_definition_str = ",".join(["{} {}".format(c.name, c.cass_type) for c in columns + [ID_COLUMN]])
        create_table_query = "CREATE TABLE IF NOT EXISTS {}({}, PRIMARY KEY ({}))".format(name, column_definition_str,
                                                                                          ID_COLUMN.name)
        connection.execute(create_table_query)

    def drop_table(self, name: str) -> None:
        connection = self._client.connect("todb")
        connection.execute("DROP TABLE {}".format(name))

    def insert_into(self, table_name: str, objects: List[Dict[str, Any]]) -> bool:
        if objects:
            if len(objects) < MAX_STATEMENTS_IN_BATCH:
                try:
                    sorted_col_names = sorted(objects[0].keys())
                    connection = self._client.connect("todb")
                    prep_statement_str = self._build_batch_insert(sorted_col_names, len(objects), table_name)
                    values = self._build_value_list(objects, sorted_col_names)

                    prep_statement = connection.prepare(prep_statement_str)
                    connection.execute(prep_statement, values)
                    return True
                except Exception as e:
                    print("Could not insert {} objects into Cassandra DB: {}".format(len(objects), e))
                    return False
            else:
                print("Cassandra can not take up insertion of {} objects!".format(len(objects)))
                return False
        else:
            return True

    def count(self, table_name: str) -> int:
        connection = self._client.connect("todb")
        result = connection.execute("SELECT COUNT(*) FROM {}".format(table_name))
        return int(result[0][0])

    def _build_value_list(self, objects, sorted_col_names):
        value_obj = []
        for o in objects:
            cells_as_str = ";".join([str(o[c]) for c in sorted_col_names])
            object_cells = [self._gen_uuid(str(cells_as_str))] + [o[c] for c in sorted_col_names]
            value_obj.extend(object_cells)
        return value_obj

    def _build_batch_insert(self, sorted_col_names: List[str], obj_count, table_name):
        single_insert_str = "INSERT INTO {} (id,{}) VALUES (?,{})".format(table_name,
                                                                          ",".join(sorted_col_names),
                                                                          ",".join(["?" for _ in sorted_col_names]))
        prep_statement_str = "BEGIN BATCH {} APPLY BATCH".format(
            ";".join([single_insert_str for _ in range(obj_count)]))
        return prep_statement_str

    def _gen_uuid(self, content: str) -> str:
        return str(uuid5(SEED_TEXT, str(content)))
