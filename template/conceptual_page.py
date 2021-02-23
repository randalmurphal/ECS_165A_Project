# from config import *
from page import Page
import numpy as np
# 512 total records
'''
0: Indirection
1: RID column
2: TimeStamp Column
3: Schema Encoding
'''
class ConceptualPage:

    def __init__(self, columns):
        self.pages = []
        self.num_records = 0
        self.add_columns(columns)
        self.path = None
        self.isPinned = False
        self.isDirty = False
        self.path = None
        # Create rid value for base/tail pages

    def update_num_records(self,page):
        self.num_records += 1

    def full(self):
        if self.num_records >= 512:
            return True
        # Check if Full, so if bytes go over 4096 or 4 KB
        return False

    def add_columns(self, columns):
        self.pages.append({}) # Indirection column
        for i in range(1, len(columns) + 4):
            self.pages.append(Page())
        self.pages[3] = np.zeros(len(columns)) # Schema Enc column

    def get_page_num(self):
        return self.num_records % 4096 // 512

    def update_RID(self):
        pass
