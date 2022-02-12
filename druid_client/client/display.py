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

from .text_table import TextTable

TEXT_TABLE = 0
HTML_TABLE = 1

class Display:

    def __init__(self):
        self.format = TEXT_TABLE
        self.html_initialized = False

    def text(self):
        self.format = TEXT_TABLE

    def html(self):
        self.format = HTML_TABLE
        if not self.html_initialized:
            from .html_table import styles
            styles()
            self.html_initialized = True
    
    def table(self):
        if self.format == HTML_TABLE:
            from .html_table import HtmlTable
            return HtmlTable()
        else:
            return TextTable()
