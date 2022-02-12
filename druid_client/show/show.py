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

from ..client.util import sql_equality

class Reports:

    def __init__(self, client):
        self.cluster = client.cluster()

    def client(self):
        return self.cluster.client()

    def as_text(self):
        self.client().show_text()

    def as_html(self):
        self.client().show_html()

    def version(self):
        print(self.client().version())

    def sql(self, sql):
        self.client().sql_query(sql).show()

    def tables(self, schema='druid'):
        self.sql(
            '''
            SELECT *
            FROM "INFORMATION_SCHEMA"."TABLES"
            WHERE "TABLE_CATALOG" {}
            '''.format(sql_equality(schema)))

    def all_tables(self):
        self.sql(
            '''
            SELECT *
            FROM "INFORMATION_SCHEMA"."TABLES"
            ''')

    def servers(self):
        self.sql('SELECT * FROM sys.servers')
