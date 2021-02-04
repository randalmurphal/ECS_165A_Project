# from config import *
from page import Page
import numpy as np
# 4096 total records
class ConceptualPage:

    def __init__(self, columns):
        self.pages = []
        self.num_records = 0

        # self.pages.append({}) # Indirection Column
        # # self.pages.append([]) # Schema Encoding
        # for i in range(1, len(columns) + 4):
        #     self.add_column()
        self.add_columns(columns)

    def update_num_records(self,page):
        self.num_records += 1

    def full(self):
        if self.num_records >= 4096:
            return True
        # Check if Full, so if bytes go over 4096 or 4 KB
        return False

    def add_columns(self, columns):
        self.pages.append({})
        for i in range(1, len(columns) + 4):
            self.pages.append([Page()])

        self.pages[3] = [np.zeros(len(columns))]

    def get_page_num(self):
        return self.num_records % 4096 // 512

    # def add_page(self, page):
    #     self.pages.append(Page())

    def update_RID(self):
        pass
