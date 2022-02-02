# Copyright 2022 Paul Rogers
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from ..client import consts
from ..client.util import sql_equality
from .table import TableMetadata

class ClusterMetadata:
    """
    Provides cluster-level metadata.

    Uses the Druid system tables (preferentially), or the lower-level REST
    calls (when necessary) to provide the metadata. Use this class to avoid
    the need to work with the system tables or lower-level APIs directly.

    Table-level metadata is in the `Table` class, available here, or
    from the client, via `table(table_name)`.
    """

    def __init__(self, cluster):
        self._cluster = cluster
        self._client = cluster.client()

    def _coord(self):
        return self._cluster.coord()

    def _overlord(self):
        return self._cluster.overlord()

    #-------- Servers --------

    def servers(self):
        return self._client.sql(
            '''
            SELECT *
            FROM {}
            ''',
            consts.SERVERS_TABLE)

    def servers_with_role(self, server_role):
        return self._client.sql(
            '''
            SELECT *
            FROM {}
            WHERE "server_type" {}
            ''',
            consts.SERVERS_TABLE,
            sql_equality(server_role))
    
    #-------- Tables --------

    def schemas(self):
        '''
        Returns the list of DB schemata. Returns a Druid SQL result set: a list of
        dictionaries with column names as keys.

        See the Schemata table: https://druid.apache.org/docs/latest/querying/sql.html#schemata-table
        '''
        return self._client.sql(
            'SELECT * FROM {}',
            consts.SCHEMAS_TABLE)

    def schema_names(self):
        results = self._client.sql(
            'SELECT * FROM {}',
            consts.SCHEMAS_TABLE)
        return [row['SCHEMA_NAME'] for row in results]

    def all_tables(self):
        return self._client.sql(
            'SELECT * FROM {}', 
            consts.TABLES_TABLE)
    
    def tables_for_schema(self, schema):
        """
        Returns the metadata for user-defined tables (data sources).
        """
        return self._client.sql(
            'SELECT * FROM {} WHERE "TABLE_SCHEMA" {}', 
            consts.TABLES_TABLE,
            sql_equality(schema))

    def tables(self, include_sys=False):
        if include_sys:
            return self.all_tables()
        else:
            return self.tables_for_schema(consts.DRUID_SCHEMA)

    def table_names(self):
        """
        Returns the names of user-defined tables (data sources).

        Equivalent to  `/druid/coordinator/v1/metadata/datasources`
        """
        rows = self._client.sql(
            'SELECT "TABLE_NAME" FROM {} WHERE "TABLE_SCHEMA" {}', 
            consts.TABLES_TABLE,
            sql_equality(consts.DRUID_SCHEMA))
        return [row['TABLE_NAME'] for row in rows]

    def table_details(self):
        """
        Returns the Coordinator metadata for all user-defined tables.
        """
        return self._coord().data_source_details()

    def table_properties(self, full=False):
        """
        Returns the Coordinator properties for all user-defined tables.
        """
        return self._coord().data_source_propertiess(full)

    def table(self, table_name) -> TableMetadata:
        """
        Returns an object which provides metadata for the specified table.
        """
        return self._cluster.table(table_name)

    #-------- Segment Allocation --------

    def loaded_segments(self):
        return self._client.sql(
            'SELECT * FROM {}',
            consts.SERVER_SEGMENTS_TABLE)
    
    def servers_for_segment(self, segment_id):
        results = self._client.sql(
            'SELECT "server" FROM {} WHERE "segment_id" {}',
            consts.SERVER_SEGMENTS_TABLE,
            sql_equality(segment_id))
        return [row['server'] for row in results]
    
    def segments_for_server(self, server):
        results = self._client.sql(
            'SELECT "segment_id" FROM {} WHERE "server" {}',
            consts.SERVER_SEGMENTS_TABLE,
            sql_equality(server))
        return [row['segment_id'] for row in results]

    #-------- Misc --------

    def client(self):
        return self._client

    def cluster(self):
        return self._cluster

    def version(self):
        return self._client.version()

