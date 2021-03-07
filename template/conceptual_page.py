# from config import *
from template.page import Page
import numpy as np
# 4096 total records
'''
0: Indirection
1: RID column
2: TimeStamp Column
3: Schema Encoding
4: TPS
5: base_RID
6: Tail_RID
'''
class ConceptualPage:

    def __init__(self, columns):
        self.pages       = []
        self.num_records = 0
        self.path        = ''
        self.isPinned    = 0
        self.dirty       = True
        self.merge_num   = 1
        self.add_columns(columns)

    def update_num_records(self):
        self.num_records += 1

    def full(self):
        if self.num_records >= 512:
            return True
        # Check if Full, so if bytes go over 4096 or 4 KB
        return False

    def add_columns(self, columns):
        self.pages.append({})
        for i in range(1, len(columns) + 6):
            self.pages.append(Page())
        self.pages[3] = [np.zeros(len(columns))]
