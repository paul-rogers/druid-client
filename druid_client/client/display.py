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

ALIGN_LEFT = 0
ALIGN_CENTER = 1
ALIGN_RIGHT = 2

alignments = ['', '^', '>']

def padded(array, width, fill):
    if array is not None and len(array) >= width:
        return array
    if array is None:
        result = []
    else:
        result = array.copy()
    return pad(result, width, fill)
    
def pad(array, width, fill):
    for _ in range(len(array), width):
        array.append(fill)
    return array

def simple_table(table_def):
    fmt = ' '.join(table_def.formats)
    table = []
    if table_def.headers is not None:
        table.append(fmt.format(*table_def.headers))
    for row in table_def.rows:
        table.append(fmt.format(*row))
    return table

def border_table(table_def):
    fmt = ' | '.join(table_def.formats)
    table = []
    if table_def.headers is not None:
        table.append(fmt.format(*table_def.headers))
        bar = ''
        for i in range(table_def.width):
            width = table_def.widths[i]
            if i > 0:
                bar += '+'
            if table_def.width == 1:
                pass
            elif i == 0:
                width += 1
            elif i == table_def.width - 1:
                width += 1
            else:
                width += 2
            bar += '-' * width
        table.append(bar)
    for row in table_def.rows:
        table.append(fmt.format(*row))
    return table

class TableDef:

    def __init__(self):
        self.width = None
        self.widths = None
        self.headers = None
        self.align = None
        self.formats = None
        self.rows = None

    def set_headers(self, headers):
        if headers is None:
            self.headers = None
            return
        has_none = False
        for i in range(len(headers)):
            if headers[i] is None:
                has_none = True
                break
        if len(headers) >= self.width and not has_none:
            self.headers = headers
            return
        self.headers = headers.copy()
        if has_none:
            for i in range(len(self.headers)):
                if self.headers[i] is None:
                    self.headers[i] = ''
        for i in range(len(headers), self.width):
            self.headers.append('')

    def find_widths(self):
        self.widths = [0 for i in range(self.width)]
        if self.headers is not None:
            for i in range(len(self.headers)):
                self.widths[i] = len(self.headers[i])
        for row in self.rows:
            for i in range(len(row)):
                self.widths[i] = max(self.widths[i], len(row[i]))

    def apply_widths(self, widths):
        if widths is None:
            return
        for i in range(min(len(self.widths), len(widths))):
            if widths[i] is not None:
                self.widths[i] = widths[i]

    def define_row_formats(self):
        self.formats = []
        for i in range(self.width):
            f = '{{:{}{}.{}}}'.format(
                alignments[self.align[i]],
                self.widths[i], self.widths[i])
            self.formats.append(f)

    def format_header(self):
        if self.headers is None:
            return None
        return self.format_row(self.headers)

    def format_row(self, data_row):
        row = []
        for i in range(self.width):
            row.append(self.formats[i].format(data_row[i]))
        return row

class TextTable:

    def __init__(self):
        self.fmt = simple_table
        self._headers = None
        self._align = None
        self._col_fmt = None
        self._widths = None
        self.sample_size = 10

    def headers(self, headers):
        self._headers = headers

    def alignments(self, align):
        self._align = align

    def widths(self, widths):
        self._widths = widths
    
    def col_format(self, col_fmt):
        self._col_fmt = col_fmt

    def compute_def(self, rows):
        table_def = TableDef()
        min_width, max_width = self.row_width(rows)
        table_def.width = max_width
        table_def.set_headers(self._headers)
        table_def.rows = self.format_rows(rows, min_width, max_width)
        table_def.find_widths()
        table_def.apply_widths(self._widths)
        table_def.align = self.find_alignments(rows, max_width)
        table_def.define_row_formats()
        return table_def

    def format(self, rows):
        return self.formatter(self.compute_def(rows))
    
    def to_string(self, rows):
        return '\n'.join(self.format(rows))
    
    def row_width(self, rows):
        max_width = 0
        min_width = None
        if self._headers is not None:
            max_width = len(self._headers)
            min_width = max_width
        for row in rows:
            max_width = max(max_width, len(row))
            min_width = max_width if min_width is None else min(min_width, max_width)
        min_width = max_width if min_width is None else min_width
        return (min_width, max_width)

    def format_rows(self, rows, min_width, max_width):
        if self._col_fmt is None:
            if min_width == max_width:
                return rows
            else:
                return self.pad_rows(rows, max_width)
        fmts = self._col_fmt
        if len(fmts) < max_width:
            fmts = fmts.copy()
            for i in range(len(fmts), max_width):
                fmts.append(lambda v: v)
        new_rows = []
        for row in rows:
            new_row = []
            for i in range(len(row)):
                new_row.append(fmts[i](row[i]))
            new_rows.append(pad(new_row, max_width, None))
        return new_rows

    def pad_rows(self, rows, width):
        new_rows = []
        for row in rows:
            new_rows.append(padded(row, width, None))
        return new_rows

    def find_alignments(self, rows, width):
        align = padded(self._align, width, None)
        unknown_count = 0
        for v in self._align:
            if v is None:
                unknown_count += 1
        if unknown_count == 0:
            return align
        for row in rows:
            for i in range(len(row)):
                if align[i] is not None:
                    continue
                v = row[i]
                if v is None:
                    continue
                if type(v) is str:
                    align[i] = ALIGN_LEFT
                else:
                    align[i] = ALIGN_RIGHT
                unknown_count -= 1
                if unknown_count == 0:
                    return align
        for i in range(width):
            if align[i] is None:
                align[i] = ALIGN_LEFT
        return align
