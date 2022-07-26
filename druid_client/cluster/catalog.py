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

class Catalog:

    def __init__(self, coord):
        self.coord = coord
 
    def create_table(self, table_metadata, ifNotExists=False):
        return self.coord.create_catalog_table(table_metadata, ifNotExists=ifNotExists)

    def update_table(self, table_metadata):
        return self.update_table_spec(table_metadata['dbSchema'], table_metadata['name'], table_metadata['defn'])

    def update_table_spec(self, schema, table_name, spec, version=0):
        return self.coord.update_catalog_table_spec(schema, table_name, spec, version=version)

    def drop_table(self, schema, table, ifExists=False):
        self.coord.drop_catalog_table(schema, table, ifExists=ifExists)

    def table(self, schema, table):
        return self.coord.catalog_table(schema, table)

    def schema_names(self):
        return self.coord.catalog_schema_names()

    def table_names(self, schema=None):
        if schema is None:
            return self.coord.catalog_table_names()
        else:
            return self.coord.catalog_table_names_in_schema(schema)

    def table_details(self, schema):
        return self.coord.catalog_table_details_in_schema(schema)
