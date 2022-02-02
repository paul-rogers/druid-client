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

class TableMetadata:
    """
    Provides table-level metadata.

    Uses the Druid system tables (preferentially), or the lower-level REST
    calls (when necessary) to provide the metadata. Use this class to avoid
    the need to work with the system tables or lower-level APIs directly.
    """

    def __init__(self, client, table_name):
        self.client = client
        self._name = table_name

    def _coord(self):
        return self.client.cluster().coord()

    def _overlord(self):
        return self.client.cluster().overlord()
    
    def name(self):
        return self._name

    def details(self):
        """
        Returns the Coordinator metadata details for this table.

        Most of this information is also available from the system tables.
        See `schema_entry()`.
        """
        return self._coord().details_for_data_source(self._name)

    def properties(self, full=False):
        """
        Returns the Coordinator data source properties for this table.

        Most of this information is also available from the system tables.
        See `schema_entry()`.
        """
        return self._coord().data_source_properties_for(self._name, full)
    
    def schema_entry(self):
        return self.client.sql(
            '''
            SELECT *
            FROM {}
            WHERE "TABLE_SCHEMA" {}
              AND "TABLE_NAME" {}
            ''',
            consts.TABLES_TABLE,
            sql_equality(consts.DRUID_SCHEMA),
            sql_equality(self._name))

    def segment_list(self, full=False):
        """
        Returns the list of segments directly from the Coordinator.

        Most of the information returned here is also available from the
        `sys.segments` system table. See `segments()`.
        """
        return self._coord().segments_for_data_source(self._name, full)
    
    def segment_details(self, segment_id):
        """
        Returns the details of a single segment directly from the Coordinator.

        Most of the information returned here is also available from the
        `sys.segments` system table. See `segment(segment_id)`.
        """
        return self._coord().segment_metadata(self._name, segment_id)

    def segments(self):
        """
        Returns the rows from `sys.segments` for this table.
        """
        return self.client.sql(
            'SELECT * FROM {} WHERE "datasource" {}',
            consts.SEGMENTS_TABLE,
            sql_equality(self._name))
    
    def segment(self, segment_id):
        """
        Returns the row from `sys.segments` for the specified segment.

        The segment is constrained to be within this table.
        """
        return self.client.sql(
            '''
            SELECT *
            FROM {}
            WHERE "datasource" {}
              AND "segment_id" {}
            ''',
            consts.SEGMENTS_TABLE,
            sql_equality(self._name),
            sql_equality(segment_id))
    
    def intervals_details(self, option=None):
        return self._coord().intervals(self._name, option)

    def interval_details(self,  interval_id, option=None):
        return self._coord().interval(self._name, interval_id, option)

    def interval_servers(self,interval_id):
        return self._coord().interval_servers(self._name, interval_id)

    def tiers(self):
        return self._coord().tiers_for(self._name)
    
    def columns(self):
        return self.client.sql(
            '''
            SELECT *
            FROM {}
            WHERE "TABLE_SCHEMA" {}
              AND "TABLE_NAME" {}
            ''',
            consts.COLUMNS_TABLE,
            sql_equality(consts.DRUID_SCHEMA),
            sql_equality(self._name))
     
    #-------- Tasks --------

    def tasks(self, state=None, type=None, max=None, created_time_interval=None):
        return self._overlord().tasks(table=self._name,
                state=state, type=type, max=max, created_time_interval=created_time_interval)

    def shut_down_tasks(self):
        return self._overlord().shut_down_tasks_for(self._name)
