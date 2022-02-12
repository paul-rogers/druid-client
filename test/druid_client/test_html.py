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

import unittest
from druid_client.client.html_table import HtmlTable
from druid_client.client.base_table import ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT

class TestHtml(unittest.TestCase):

    def test_empty(self):
        table = HtmlTable()
        html = table.format([])
        expected = '''
           <table>
           </table>
        '''
        self.verify(expected, html)

    def test_no_header(self):
        table = HtmlTable()
        rows = [['a', '10'], ['b', '20']]
        html = table.format(rows)
        expected = '''
           <table>
             <tr><td>a</td><td>10</td></tr>
             <tr><td>b</td><td>20</td></tr>
           </table>
        '''
        self.verify(expected, html)

    def test_with_header(self):
        table = HtmlTable()
        table.headers(['x', 'y'])
        rows = [['a', '10'], ['b', '20']]
        html = table.format(rows)
        expected = '''
           <table>
             <tr><th>x</th><th>y</th></tr>
             <tr><td>a</td><td>10</td></tr>
             <tr><td>b</td><td>20</td></tr>
           </table>
        '''
        self.verify(expected, html)

    def test_align(self):
        table = HtmlTable()
        table.headers(['x', 'y', 'z'])
        table.alignments([ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT])
        rows = [['a', 'aa', '10'], ['b', 'bb', '20']]
        html = table.format(rows)
        expected = '''
           <table>
             <tr><th class="druid-left">x</th><th class="druid-center">y</th><th class="druid-right">z</th></tr>
             <tr><td class="druid-left">a</td><td class="druid-center">aa</td><td class="druid-right">10</td></tr>
             <tr><td class="druid-left">b</td><td class="druid-center">bb</td><td class="druid-right">20</td></tr>
           </table>
        '''
        self.maxDiff = None
        self.verify(expected, html)

    def verify(self, s1, s2):
        a1 = self.to_lines(s1)
        a2 = self.to_lines(s2)
        self.assertEqual(a1, a2)

    def to_lines(self, value):
        if value is None:
            return []
        lines = []
        for line in value.split('\n'):
            line = line.strip()
            if len(line) > 0:
                lines.append(line)
        return lines

if __name__ == '__main__':
    unittest.main()