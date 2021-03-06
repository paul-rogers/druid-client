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
from druid_client.client.text_table import TextTable, TableDef, simple_table, border_table
from druid_client.client.util import pad, padded
from druid_client.client.base_table import ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT

class TestDisplay(unittest.TestCase):

    def testPad(self):

        a = []
        out = pad(a, 2, 'x')
        self.assertIs(a, out)
        self.assertEqual(['x', 'x'], out)

        a = None
        out = padded(a, 2, 'x')
        self.assertEqual(['x', 'x'], out)

        self.assertIs(out, padded(out, 2, 'x'))
        self.assertIs(out, padded(out, 1, 'x'))
        self.assertEqual(['x', 'x'], out)

        out2 = padded(out, 3, 10)
        self.assertIsNot(out, out2)
        self.assertEqual(['x', 'x', 10], out2)

    def testFindWidths(self):

        d = TableDef()
        d.width = 0
        d.headers = None
        d.rows = []
        d.find_widths()
        self.assertEqual([], d.widths)

        d = TableDef()
        d.width = 3
        d.headers = ['a', 'bb', '']
        d.rows = []
        d.find_widths()
        self.assertEqual([1, 2, 0], d.widths)

        d = TableDef()
        d.width = 3
        d.headers = ['a', 'bb', '']
        d.rows = [['', 'bbb', 'c']]
        d.find_widths()
        self.assertEqual([1, 3, 1], d.widths)

        d = TableDef()
        d.width = 3
        d.headers = ['a', 'b', 'c']
        d.rows = [['', 'bbb', 'c'], ['aa', '', 'cc']]
        d.find_widths()
        self.assertEqual([2, 3, 2], d.widths)

    def testApplyWidths(self):

        d = TableDef()
        d.width = 3
        d.widths = [1, 2, 3]
        d.apply_widths(None)
        self.assertEqual([1, 2, 3], d.widths)

        d = TableDef()
        d.width = 3
        d.widths = [1, 2, 3]
        d.apply_widths(None)
        self.assertEqual([1, 2, 3], d.widths)

        d = TableDef()
        d.width = 3
        d.widths = [1, 2, 3]
        d.apply_widths([None, 1, 2])
        self.assertEqual([1, 1, 2], d.widths)

        d = TableDef()
        d.width = 3
        d.widths = [1, 2, 3]
        d.apply_widths([3, 4])
        self.assertEqual([3, 4, 3], d.widths)

    def testRowFormats(self):

        d = TableDef()
        d.width = 3
        d.align = [ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT]
        d.widths = [5, 7, 9]
        d.width = 3
        d.define_row_formats()
        self.assertEqual(['{:5.5}', '{:^7.7}', '{:>9.9}'], d.formats)

        fmt = '|'.join(d.formats)
        out = fmt.format('aa', 'bb', 'cc')
        self.assertEqual('aa   |  bb   |       cc', out)
        out = fmt.format('abcdefg', 'hijklmno', 'pqrstuvwxyz')
        self.assertEqual('abcde|hijklmn|pqrstuvwx', out)
        self.assertEqual(['aa   ', '  bb   ', '       cc'], d.format_row(['aa', 'bb', 'cc']))

    def testSimpleTable(self):

        d = TableDef()
        d.width = 3
        d.align = [ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT]
        d.widths = [5, 7, 9]
        d.width = 3
        d.headers = ['a', 'b', 'c']
        d.define_row_formats()
        d.rows = [
            ['x', 'y', 'z'],
            ['abcdefg', 'hijklmno', 'pqrstuvwxyz']
        ]
        out = simple_table(d)
        expected = [
            'a        b            c',
            'x        y            z',
            'abcde hijklmn pqrstuvwx'
        ]
        self.assertEqual(expected, out)

    def testBorderTable(self):

        d = TableDef()
        d.width = 3
        d.align = [ALIGN_LEFT, ALIGN_CENTER, ALIGN_RIGHT]
        d.widths = [5, 7, 9]
        d.width = 3
        d.headers = ['a', 'b', 'c']
        d.define_row_formats()
        d.rows = [
            ['x', 'y', 'z'],
            ['abcdefg', 'hijklmno', 'pqrstuvwxyz']
        ]
        out = border_table(d)
        expected = [
            'a     |    b    |         c',
            '------+---------+----------',
            'x     |    y    |         z',
            'abcde | hijklmn | pqrstuvwx'
        ]
        self.assertEqual(expected, out)

    def testEmptyTable(self):
        t = TextTable()
        out = t.format(None)
        self.assertEqual('', out)

        t = TextTable()
        out = t.format([])
        self.assertEqual('', out)

        t = TextTable()
        t.headers = []
        out = t.format([])
        self.assertEqual('', out)

    def testNoHeaders(self):
        t = TextTable()
        out = t.format([['a'], ['ccc', 'd']])
        expected = [
            'a    ',
            'ccc d'
        ]
        self.assertEqual(expected, out.split('\n'))

        t = TextTable()
        t.widths([4, 3])
        out = t.format([['a'], ['ccc', 'd']])
        expected = [
            'a       ',
            'ccc  d  '
        ]
        self.assertEqual(expected, out.split('\n'))

        t = TextTable()
        t.widths([4, 3])
        t.alignments([ALIGN_CENTER, ALIGN_RIGHT])
        out = t.format([['a'], ['ccc', 'd']])
        expected = [
            ' a      ',
            'ccc    d'
        ]
        self.assertEqual(expected, out.split('\n'))

        t = TextTable()
        t.widths([4, 3])
        t.alignments([ALIGN_CENTER, ALIGN_RIGHT])
        out = t.format([[1], [333, 4]])
        expected = [
            ' 1      ',
            '333    4'
        ]
        self.assertEqual(expected, out.split('\n'))

if __name__ == '__main__':
    unittest.main()