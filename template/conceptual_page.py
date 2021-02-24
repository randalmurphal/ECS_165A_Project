# from config import *
from page import Page
import numpy as np
# 512 total records
'''
0: Indirection
1: RID column
2: TimeStamp Column
3: Schema Encoding Column
4: TPS Column
5: BaseRID Column
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
        for i in range(1, len(columns) + 6):
            self.pages.append(Page())
        self.pages[3] = [np.zeros(len(columns))] # Schema Enc column

    def insert_record(self, record):
        self.pages[1].write(record.rid)
        self.pages[2].write(record.time)
        self.pages[3].append(np.zeros(len(base_page.pages) - 6))
        self.pages[4].write(record.TPS)
        self.pages[5].write(record.baseRID)
        for i, col in enumerate(record.columns):
            self.pages[i+6].write(col)
        
        self.num_records += 1
        pass
